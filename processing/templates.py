"""Drain template mining shared by the batch trainer, the streaming detector and the evaluation.

One Drain miner per service, trained in batch and saved to /state/drain/<service>.bin.
The stream only *matches* against the frozen miners: Spark runs many Python workers in
parallel and a learning miner per worker would hand out conflicting template IDs.
A line that matches no trained template gets a deterministic `new-<hash>` ID built from
its masked text, so every worker agrees on it; that is what the new-template rule watches.
"""
import hashlib
import os
import re

from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig

STATE_DIR = os.getenv("DRAIN_STATE_DIR", "/state/drain")

# Raw Loghub line -> (level, message) for batch jobs; mirrors the Logstash grok patterns.
RAW_FORMATS = {
    "hdfs": r"^\d{6} \d{6} \d+ (\w+) [^:]+: (.*)$",
    "openssh": r"^\w{3}\s+\d+ \d\d:\d\d:\d\d \S+ [^\[:]+(?:\[\d+\])?: ()(.*)$",  # no level in syslog
    "apache": r"^\[[^\]]+\] \[(\w+)\] (.*)$",
}


def drain_config():
    c = TemplateMinerConfig()
    c.drain_sim_th = 0.4
    c.drain_depth = 4
    c.profiling_enabled = False
    c.mask_prefix, c.mask_suffix = "<", ">"
    c.masking_instructions = [
        MaskingInstruction(r"blk_-?\d+", "BLK"),
        MaskingInstruction(r"((?<=[^A-Za-z0-9])|^)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(:\d+)?((?=[^A-Za-z0-9])|$)", "IP"),
        MaskingInstruction(r"((?<=[^A-Za-z0-9])|^)(0x[a-fA-F0-9]+)((?=[^A-Za-z0-9])|$)", "HEX"),
        MaskingInstruction(r"((?<=[^A-Za-z0-9])|^)([\-\+]?\d+)((?=[^A-Za-z0-9])|$)", "NUM"),
    ]
    return c


def state_path(service):
    return os.path.join(STATE_DIR, f"{service}.bin")


def new_miner():
    return TemplateMiner(config=drain_config())


def save_miner(miner, service):
    os.makedirs(STATE_DIR, exist_ok=True)
    path = state_path(service)
    if os.path.exists(path):
        os.remove(path)
    miner.persistence_handler = FilePersistence(path)
    miner.save_state("trained")


_miners = {}  # per Python worker process, loaded once


def get_miner(service):
    if service not in _miners:
        path = state_path(service)
        _miners[service] = TemplateMiner(FilePersistence(path), drain_config()) if os.path.exists(path) else None
    return _miners[service]


_digits = re.compile(r"\d+")


def assign(service, message):
    """(template_id, template) for one log message. Deterministic across workers."""
    message = message or ""
    miner = get_miner(service)
    if miner is not None:
        cluster = miner.match(message, full_search_strategy="fallback")
        if cluster is not None:
            return f"{service}-{cluster.cluster_id}", cluster.get_template()
        masked = miner.masker.mask(message)
    else:
        masked = new_miner().masker.mask(message)
    masked = _digits.sub("<NUM>", masked)
    digest = hashlib.sha1(f"{service}|{masked}".encode()).hexdigest()[:10]
    return f"new-{service}-{digest}", masked


if __name__ == "__main__":
    # Self-check: masking makes variable values collapse to one template; IDs are stable.
    m = new_miner()
    for line in ["Receiving block blk_-160 src: /10.0.0.1:54106 dest: /10.0.0.2:50010",
                 "Receiving block blk_777 src: /10.0.0.9:1 dest: /10.0.0.3:50010"]:
        m.add_log_message(line)
    assert len(m.drain.clusters) == 1, m.drain.clusters
    _miners["t"] = m
    tid, tpl = assign("t", "Receiving block blk_42 src: /1.2.3.4:5 dest: /5.6.7.8:9")
    assert tid == "t-1" and "<BLK>" in tpl and "<IP>" in tpl, (tid, tpl)
    a = assign("t", "Totally unseen error 1234 at 0xdead")
    b = assign("t", "Totally unseen error 99 at 0xbeef")
    assert a[0] == b[0] and a[0].startswith("new-t-"), (a, b)
    print("templates.py self-check OK")
