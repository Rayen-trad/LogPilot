"""LogPilot streaming detector (Spark Structured Streaming).

logs.parsed -> dedup -> Drain template_id -> counts per (window, service, template, level)
            -> foreachBatch: EWMA baselines + rules -> Kafka metrics.windows / anomalies, Redis.

Rules (README "Detection logic"):
  zscore       count far above the key's EWMA baseline (after warm-up)
  new_template a WARN/ERROR/FATAL line matching no trained template (e.g. a faulty release)
  error_ratio  a service's ERROR share jumps well above its own baseline

Counts are emitted in update mode, so a spike is flagged while its window is still open
(detection delay ~ trigger interval, not window + watermark).
"""
import hashlib
import json
import math
import os
from datetime import datetime, timezone

import redis
from pyspark.sql import SparkSession, functions as F, types as T

import templates

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
WINDOW = os.getenv("SPARK_WINDOW_DURATION", "1 minute")
WATERMARK = os.getenv("SPARK_WATERMARK_DURATION", "2 minutes")
TRIGGER = os.getenv("SPARK_TRIGGER", "5 seconds")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_DIR", "/state/checkpoints") + "/detect"
MAX_PER_TRIGGER = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "200000")

Z = float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "3.0"))
MIN_COUNT = int(os.getenv("ANOMALY_MIN_COUNT", "10"))           # ignore spikes from 1 to 4 lines
WARMUP = int(os.getenv("BASELINE_WARMUP_WINDOWS", "5"))         # windows before a key can alert
ALPHA = float(os.getenv("BASELINE_EWMA_ALPHA", "0.1"))
ERR_DELTA = float(os.getenv("ERROR_RATIO_DELTA", "0.3"))        # absolute jump in ERROR share
MAX_GAP_FILL = 60                                               # zero-windows replayed into a baseline

ERROR_LEVELS = {"ERROR", "FATAL"}
ALERT_LEVELS = {"WARN", "ERROR", "FATAL"}


def window_seconds(spec):
    n, unit = spec.split()
    return int(n) * {"second": 1, "minute": 60, "hour": 3600}[unit.rstrip("s")]


W = window_seconds(WINDOW)


def iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


class Baseline:
    """EWMA mean/variance of one key's per-window value, fed only with finished windows."""

    def __init__(self, d=None):
        d = d or {}
        self.mean, self.var, self.n = d.get("mean", 0.0), d.get("var", 0.0), d.get("n", 0)
        self.win, self.value, self.alerted = d.get("win"), d.get("value", 0.0), d.get("alerted")

    def to_dict(self):
        return self.__dict__

    def _learn(self, x):
        self.n += 1
        if self.n == 1:
            self.mean, self.var = float(x), 0.0
        else:
            d = x - self.mean
            self.mean += ALPHA * d
            self.var = (1 - ALPHA) * (self.var + ALPHA * d * d)

    def observe(self, win, value, fill_gaps=True):
        """Record the running value of window `win`. False if the update is for an old window."""
        if self.win is None or win > self.win:
            if self.win is not None:
                self._learn(self.value)
                if fill_gaps:
                    for _ in range(min((win - self.win) // W - 1, MAX_GAP_FILL)):
                        self._learn(0)
            self.win, self.value = win, value
        elif win == self.win:
            self.value = value
        else:
            return False
        return True

    @property
    def std(self):
        return math.sqrt(max(self.var, 0.0))

    def zscore(self):
        return (self.value - self.mean) / max(self.std, 1.0)  # floor: quiet keys have std ~ 0


class Detector:
    def __init__(self, r):
        self.r = r
        self.base = {k: Baseline(json.loads(v)) for k, v in r.hgetall("baseline").items()}
        self.windows = {}       # (service, win) -> {(template_id, level): count}  for error ratio
        self.catalog = set(r.hkeys("templates:catalog"))
        print(f"[detect] restored {len(self.base)} baselines from Redis", flush=True)

    def process(self, rows):
        metrics, anomalies, dirty, touched = [], [], set(), set()
        now = iso(datetime.now(timezone.utc).timestamp())

        for row in sorted(rows, key=lambda r: r["win"]):
            svc, tid, lvl, win, count = row["service"], row["template_id"], row["level"], row["win"], row["count"]
            key = f"{svc}|{tid}|{lvl}"
            b = self.base.setdefault(key, Baseline())
            if not b.observe(win, count):
                continue
            dirty.add(key)
            self.windows.setdefault((svc, win), {})[(tid, lvl)] = count
            touched.add((svc, win))

            z = b.zscore()
            metrics.append(dict(window_start=iso(win), window_end=iso(win + W), service=svc, template_id=tid,
                                template=row["template"], level=lvl, count=count,
                                baseline_mean=round(b.mean, 3), baseline_std=round(b.std, 3), zscore=round(z, 2)))
            if b.n >= WARMUP and count >= MIN_COUNT and z >= Z and b.alerted != win:
                b.alerted = win
                anomalies.append(self._anomaly("zscore", metrics[-1], now))

            if tid not in self.catalog:
                self.catalog.add(tid)
                self.r.hsetnx("templates:catalog", tid, json.dumps(
                    {"service": svc, "template": row["template"], "first_seen": iso(win)}))
            if tid.startswith("new-") and lvl in ALERT_LEVELS and self.r.sadd("templates:alerted", tid):
                anomalies.append(self._anomaly("new_template", metrics[-1], now))

        for svc, win in touched:
            counts = self.windows[(svc, win)]
            total = sum(counts.values())
            errors = sum(c for (_, lvl), c in counts.items() if lvl in ERROR_LEVELS)
            ratio = errors / total
            key = f"{svc}|__error_ratio__"
            b = self.base.setdefault(key, Baseline())
            if b.observe(win, ratio, fill_gaps=False):
                dirty.add(key)
                if b.n >= WARMUP and total >= MIN_COUNT and ratio - b.mean >= ERR_DELTA and b.alerted != win:
                    b.alerted = win
                    anomalies.append(self._anomaly("error_ratio", dict(
                        window_start=iso(win), window_end=iso(win + W), service=svc, template_id=None, template=None,
                        level="ERROR", count=errors, total=total, error_ratio=round(ratio, 3),
                        baseline_mean=round(b.mean, 3), baseline_std=round(b.std, 3), zscore=None), now))
            # live counters for the UI: count:{service}:{level}:{minute}
            per_level = {}
            for (_, lvl), c in counts.items():
                per_level[lvl] = per_level.get(lvl, 0) + c
            stamp = datetime.fromtimestamp(win, timezone.utc).strftime("%Y%m%d%H%M%S")
            pipe = self.r.pipeline()
            for lvl, c in per_level.items():
                pipe.set(f"count:{svc}:{lvl}:{stamp}", c, ex=3600)
            pipe.execute()

        if dirty:
            self.r.hset("baseline", mapping={k: json.dumps(self.base[k].to_dict()) for k in dirty})
        newest = max((w for _, w in self.windows), default=0)
        self.windows = {k: v for k, v in self.windows.items() if k[1] > newest - 10 * W}
        return metrics, anomalies

    @staticmethod
    def _anomaly(rule, m, now):
        aid = hashlib.sha1(f"{rule}|{m['service']}|{m['template_id']}|{m['level']}|{m['window_start']}".encode())
        return {**m, "anomaly_id": aid.hexdigest()[:16], "rule": rule, "detected_at": now}


def main():
    spark = (SparkSession.builder.appName("logpilot-detect")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             # dedup + aggregation in update mode; we accept the documented late-data caveat
             .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379,
                    password=os.getenv("REDIS_PASSWORD") or None, decode_responses=True)
    detector = Detector(r)

    missing = [s for s in templates.RAW_FORMATS if not os.path.exists(templates.state_path(s))]
    if missing:
        print(f"[detect] WARNING no trained templates for {missing}: run batch/train_templates.py", flush=True)

    schema = T.StructType([
        T.StructField("@timestamp", T.StringType()), T.StructField("service", T.StringType()),
        T.StructField("level", T.StringType()), T.StructField("log_message", T.StringType()),
        T.StructField("message", T.StringType()),
        T.StructField("log", T.StructType([
            T.StructField("offset", T.LongType()),
            T.StructField("file", T.StructType([T.StructField("path", T.StringType())]))])),
    ])

    @F.pandas_udf("template_id string, template string")
    def assign(service, message):
        import pandas as pd
        out = [templates.assign(s, m) for s, m in zip(service, message)]
        return pd.DataFrame(out, columns=["template_id", "template"])

    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA)
              .option("subscribe", "logs.parsed")
              # only used on a fresh checkpoint; afterwards Spark resumes from its committed offsets
              .option("startingOffsets", os.getenv("SPARK_STARTING_OFFSETS", "earliest"))
              .option("maxOffsetsPerTrigger", MAX_PER_TRIGGER)
              .option("failOnDataLoss", "false")
              .load()
              .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
              .select(F.to_timestamp(F.col("e.`@timestamp`")).alias("ts"), "e.service", "e.level",
                      F.coalesce("e.log_message", "e.message").alias("msg"),
                      # same inputs as Logstash's document fingerprint: replays collapse to one event
                      F.sha2(F.concat_ws("|", "e.log.file.path", F.col("e.log.offset").cast("string"), "e.message"),
                             256).alias("event_id"))
              .where("ts IS NOT NULL AND service IS NOT NULL")
              .withWatermark("ts", WATERMARK)
              .dropDuplicatesWithinWatermark(["event_id"]))

    counts = (events.withColumn("t", assign("service", "msg"))
              .groupBy(F.window("ts", WINDOW), "service", F.col("t.template_id").alias("template_id"),
                       F.col("t.template").alias("template"), F.coalesce("level", F.lit("UNKNOWN")).alias("level"))
              .count())

    def to_kafka(docs, topic):
        if docs:
            (spark.createDataFrame([(d["service"], json.dumps(d)) for d in docs], "key string, value string")
             .write.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("topic", topic).save())

    def handle(batch_df, batch_id):
        rows = [dict(service=r.service, template_id=r.template_id, template=r.template, level=r.level,
                     count=r["count"], win=int(r.window.start.replace(tzinfo=timezone.utc).timestamp()))
                for r in batch_df.collect()]
        if not rows:
            return
        metrics, anomalies = detector.process(rows)
        to_kafka(metrics, "metrics.windows")
        to_kafka(anomalies, "anomalies")
        for a in anomalies:
            print(f"[detect] ANOMALY {a['rule']:<12} {a['service']:<8} {a['window_start']} "
                  f"count={a['count']} z={a['zscore']} {(a['template'] or '')[:70]}", flush=True)
        print(f"[detect] batch {batch_id}: {len(rows)} window rows, {len(anomalies)} anomalies", flush=True)

    (counts.writeStream.outputMode("update").foreachBatch(handle)
     .option("checkpointLocation", CHECKPOINT).trigger(processingTime=TRIGGER)
     .start().awaitTermination())


if __name__ == "__main__":
    main()
