"""Incident manager: Kafka `anomalies` -> incidents (Redis + Kafka `incidents.events` -> ES `incidents`).

Rules (proposal, "Incident correlation"):
  merge   an anomaly joins an open incident when its service is the same as, or depends on / is a
          dependency of, a service already in it, and the incident saw an anomaly in the last MERGE s
  origin  the anomalous service none of whose dependencies are anomalous (earliest first if several)
  close   after QUIET s without a new anomaly

    python manager.py check     # self-check of the correlation rules
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

DEPENDS = {"api-gateway": ["orders"], "orders": ["payments", "inventory"]}
MERGE = int(os.getenv("INCIDENT_MERGE_SECONDS", "300"))
QUIET = int(os.getenv("INCIDENT_QUIET_SECONDS", "180"))
TTL = 7 * 86400
JSON_FIELDS = ("services", "first_seen", "rules", "anomaly_count", "last_anomaly_ts", "evidence", "ended_at")


def iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def deps(service):
    """Transitive dependencies of a service."""
    out, todo = set(), list(DEPENDS.get(service, []))
    while todo:
        d = todo.pop()
        if d not in out:
            out.add(d)
            todo += DEPENDS.get(d, [])
    return out


def related(a, b):
    return a == b or b in deps(a) or a in deps(b)


def origin(first_seen):
    """first_seen: {service: window_start}. The dependency graph is acyclic, so a root always exists."""
    roots = [s for s in first_seen if not deps(s) & first_seen.keys()]
    return min(roots, key=lambda s: (first_seen[s], s))


class Manager:
    def __init__(self, r, producer):
        self.r, self.p, self.open = r, producer, {}
        for iid in r.zrange("incidents:active", 0, -1):
            h = r.hgetall(f"incident:{iid}")
            if h:
                self.open[iid] = {k: json.loads(v) if k in JSON_FIELDS else v for k, v in h.items()}
        print(f"[incidents] restored {len(self.open)} open incidents", flush=True)

    def handle(self, a, now):
        svc = a["service"]
        inc = next((i for i in self.open.values() if now - i["last_anomaly_ts"] <= MERGE
                    and any(related(svc, s) for s in i["services"])), None)
        action = "updated"
        if inc is None:
            iid = f"INC-{self.r.incr('incident:seq'):04d}"
            inc = self.open[iid] = dict(incident_id=iid, status="open", started_at=a["window_start"],
                                        opened_at=iso(now), ended_at=None, first_seen={}, rules=[],
                                        anomaly_count=0, evidence=[])
            action = "opened"
        fs = inc["first_seen"]
        fs[svc] = min(fs.get(svc, a["window_start"]), a["window_start"])
        inc.update(services=sorted(fs), origin=origin(fs), rules=sorted(set(inc["rules"]) | {a["rule"]}),
                   anomaly_count=inc["anomaly_count"] + 1, started_at=min(inc["started_at"], a["window_start"]),
                   updated_at=iso(now), last_anomaly_ts=now)
        # a few anomalies as evidence for the copilot: what spiked, where, how much
        inc["evidence"] = (inc["evidence"] + [{k: a.get(k) for k in (
            "rule", "service", "level", "template", "count", "zscore", "error_ratio", "window_start")}])[-20:]
        self.save(inc, action)

    def close_quiet(self, now):
        for inc in [i for i in self.open.values() if now - i["last_anomaly_ts"] > QUIET]:
            del self.open[inc["incident_id"]]
            inc.update(status="closed", ended_at=iso(inc["last_anomaly_ts"]), updated_at=iso(now))
            self.save(inc, "closed")

    def save(self, inc, action):
        iid = inc["incident_id"]
        key = f"incident:{iid}"
        self.r.hset(key, mapping={k: json.dumps(v) if k in JSON_FIELDS else v for k, v in inc.items()})
        self.r.expire(key, TTL)
        if inc["status"] == "open":
            self.r.zadd("incidents:active", {iid: inc["anomaly_count"]})  # severity = anomalies so far
        else:
            self.r.zrem("incidents:active", iid)
        self.p.produce("incidents.events", key=iid, value=json.dumps({**inc, "action": action}))
        self.p.poll(0)
        print(f"[incidents] {action:<8}{iid} origin={inc['origin']} services={','.join(inc['services'])} "
              f"anomalies={inc['anomaly_count']}", flush=True)


def main():
    import redis
    from confluent_kafka import Consumer, Producer

    kafka = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), password=os.getenv("REDIS_PASSWORD") or None,
                    decode_responses=True)
    m = Manager(r, Producer({"bootstrap.servers": kafka}))
    # latest: on first start, don't turn last week's anomalies into incidents; afterwards the group resumes
    c = Consumer({"bootstrap.servers": kafka, "group.id": "incident-manager", "auto.offset.reset": "latest"})
    c.subscribe(["anomalies"])
    while True:
        msg = c.poll(1.0)
        if msg is not None and not msg.error():
            m.handle(json.loads(msg.value()), time.time())
        m.close_quiet(time.time())


def check():
    assert related("api-gateway", "payments") and related("payments", "orders")
    assert not related("payments", "inventory") and not related("hdfs", "payments")
    t = "2026-11-20T14:02:00Z"
    assert origin({"api-gateway": t, "orders": t, "payments": t}) == "payments"
    assert origin({"api-gateway": "2026-11-20T14:01:00Z", "orders": t}) == "orders"  # deeper wins over earlier

    class FakeRedis:
        def __init__(self): self.n = 0
        def incr(self, _): self.n += 1; return self.n
        def __getattr__(self, _): return lambda *a, **k: []

    class FakeProducer:
        def produce(self, *a, **k): pass
        def poll(self, _): pass

    m = Manager(FakeRedis(), FakeProducer())
    for i, svc in enumerate(["payments", "orders", "api-gateway", "payments", "hdfs"]):
        m.handle(dict(service=svc, rule="error_ratio", window_start=t), now=1000 + i)
    incs = sorted(m.open.values(), key=lambda i: i["incident_id"])
    assert [(i["origin"], i["anomaly_count"]) for i in incs] == [("payments", 4), ("hdfs", 1)], incs
    m.close_quiet(now=1004 + QUIET + 1)
    assert not m.open
    print("manager.py self-check OK")


if __name__ == "__main__":
    check() if sys.argv[1:] == ["check"] else main()
