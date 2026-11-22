"""Spark batch job: bootstrap Drain templates from the historical Loghub files.

Spark reads and samples each dataset and extracts the message part in parallel; the sampled
messages then stream to the driver, where Drain (a sequential algorithm) learns the templates.
Output: /state/drain/<service>.bin, loaded by the streaming detector and the evaluation.

    docker compose run --rm spark spark-submit /app/batch/train_templates.py
"""
import argparse
import os
import time

from pyspark.sql import SparkSession, functions as F

from templates import RAW_FORMATS, new_miner, save_miner

DATASETS = {  # service -> (file, default sample fraction ~ 50k-250k lines each)
    "hdfs": ("/data/HDFS_v1/HDFS.log", 0.02),
    "openssh": ("/data/SSH/SSH.log", 0.3),
    "apache": ("/data/Apache/Apache.log", 1.0),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--services", default=",".join(DATASETS))
    ap.add_argument("--fraction", type=float, help="override sample fraction for all services")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("logpilot-train-templates").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for service in args.services.split(","):
        path, fraction = DATASETS[service]
        fraction = args.fraction or fraction
        t0 = time.time()
        lines = spark.read.text(path)
        if fraction < 1.0:
            lines = lines.sample(fraction=fraction, seed=42)
        msgs = lines.select(F.regexp_extract("value", RAW_FORMATS[service], 2).alias("m")).where("m != ''")

        miner, n = new_miner(), 0
        for row in msgs.toLocalIterator():
            miner.add_log_message(row.m)
            n += 1
        save_miner(miner, service)

        clusters = sorted(miner.drain.clusters, key=lambda c: c.size, reverse=True)
        print(f"\n[{service}] {n:,} lines sampled ({fraction:.0%}) -> {len(clusters)} templates "
              f"in {time.time() - t0:.0f}s, saved to /state/drain/{service}.bin")
        for c in clusters[:8]:
            print(f"   {c.size:>8,}  {service}-{c.cluster_id}: {c.get_template()[:110]}")

    spark.stop()


if __name__ == "__main__":
    main()
