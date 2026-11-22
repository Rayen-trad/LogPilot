"""Spark batch job: block-level anomaly detection accuracy on the labelled HDFS_v1 dataset.

1. Parse all of HDFS.log, give each line our Drain template ID (same code as the stream).
2. Group by block ID into event-count vectors (one column per template).
3. Detect anomalous blocks with:
     pca       Xu et al. (SOSP'09): TF-IDF weighting, zero-mean, PCA keeping 95% variance,
               squared prediction error (SPE) vs. the Q-statistic threshold.
     warn_rule naive rule: block has at least one WARN/ERROR/FATAL line.
4. Compare with anomaly_label.csv: precision, recall, F1 -> /results/hdfs_accuracy.csv

    docker compose run --rm spark spark-submit --master local[4] /app/batch/evaluate_hdfs.py
"""
import csv
import math
import os
import time

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F

import templates

LOG = "/data/HDFS_v1/HDFS.log"
LABELS = "/data/HDFS_v1/preprocessed/anomaly_label.csv"
RESULTS = os.getenv("RESULTS_DIR", "/results")
VARIANCE = 0.95
C_ALPHA = 3.2905  # 1 - alpha = 0.9995, as in the loglizer reference implementation

# He et al., "Experience Report: System Log Analysis for Anomaly Detection" (ISSRE 2016), HDFS, PCA.
PUBLISHED_PCA = dict(precision=0.98, recall=0.67, f1=0.79)


def q_threshold(residual_eigenvalues):
    phi = [np.sum(residual_eigenvalues ** i) for i in (1, 2, 3)]
    h0 = 1.0 - 2.0 * phi[0] * phi[2] / (3.0 * phi[1] ** 2)
    return phi[0] * (C_ALPHA * math.sqrt(2 * phi[1] * h0 ** 2) / phi[0]
                     + 1.0 + phi[1] * h0 * (h0 - 1.0) / phi[0] ** 2) ** (1.0 / h0)


def scores(df, flag_col):
    c = df.groupBy().agg(
        F.sum(F.when(F.col(flag_col) & F.col("anomaly"), 1).otherwise(0)).alias("tp"),
        F.sum(F.when(F.col(flag_col) & ~F.col("anomaly"), 1).otherwise(0)).alias("fp"),
        F.sum(F.when(~F.col(flag_col) & F.col("anomaly"), 1).otherwise(0)).alias("fn")).first()
    p = c.tp / max(c.tp + c.fp, 1)
    r = c.tp / max(c.tp + c.fn, 1)
    return dict(tp=c.tp, fp=c.fp, fn=c.fn, precision=round(p, 4), recall=round(r, 4),
                f1=round(2 * p * r / max(p + r, 1e-9), 4))


def main():
    t0 = time.time()
    spark = (SparkSession.builder.appName("logpilot-evaluate-hdfs")
             .config("spark.sql.shuffle.partitions", "24").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    @F.pandas_udf("string")
    def template_id(msg):
        return pd.Series([templates.assign("hdfs", m)[0] for m in msg])

    lines = (spark.read.text(LOG)
             .select(F.regexp_extract("value", templates.RAW_FORMATS["hdfs"], 1).alias("level"),
                     F.regexp_extract("value", templates.RAW_FORMATS["hdfs"], 2).alias("msg"),
                     F.expr("array_distinct(regexp_extract_all(value, 'blk_-?[0-9]+', 0))").alias("blocks"))
             .where("msg != ''")
             .withColumn("template_id", template_id("msg"))
             .select("level", "template_id", F.explode("blocks").alias("block")))

    labels = (spark.read.option("header", True).csv(LABELS)
              .select(F.col("BlockId").alias("block"), (F.col("Label") == "Anomaly").alias("anomaly")))

    counts = lines.groupBy("block", "template_id").count().cache()
    ids = sorted(r.template_id for r in counts.select("template_id").distinct().collect())
    blocks = (counts.groupBy("block").pivot("template_id", ids).sum("count").na.fill(0)
              .join(lines.groupBy("block").agg(F.max(F.col("level").isin("WARN", "ERROR", "FATAL")).alias("warn_rule")),
                    "block")
              .join(labels, "block").cache())
    n = blocks.count()
    print(f"[eval] {n:,} labelled blocks, {len(ids)} templates "
          f"({sum(i.startswith('new-') for i in ids)} unmatched by training), {time.time() - t0:.0f}s", flush=True)

    # ---- PCA (Xu et al.) -------------------------------------------------------------------------
    present = blocks.agg(*[F.sum(F.when(F.col(f"`{i}`") > 0, 1).otherwise(0)).alias(i) for i in ids]).first()
    idf = np.array([math.log(n / present[i]) if present[i] else 0.0 for i in ids])
    feats = [F.col(f"`{i}`").cast("double") for i in ids]
    vec = blocks.select("block", "anomaly", "warn_rule", F.array(*feats).alias("x"))

    def partial_moments(it):
        s, xtx = np.zeros(len(ids)), np.zeros((len(ids), len(ids)))
        for pdf in it:
            x = np.stack(pdf["x"].values) * idf
            s += x.sum(0)
            xtx += x.T @ x
        yield pd.DataFrame({"s": [s.tolist()], "xtx": [xtx.ravel().tolist()]})

    parts = vec.select("x").mapInPandas(partial_moments, "s array<double>, xtx array<double>").collect()
    mu = np.sum([p.s for p in parts], axis=0) / n
    cov = np.sum([p.xtx for p in parts], axis=0).reshape(len(ids), len(ids)) / n - np.outer(mu, mu)
    eigval, eigvec = np.linalg.eigh(cov)
    eigval, eigvec = np.clip(eigval[::-1], 0, None), eigvec[:, ::-1]
    k = int(np.searchsorted(np.cumsum(eigval) / eigval.sum(), VARIANCE) + 1)
    residual = np.eye(len(ids)) - eigvec[:, :k] @ eigvec[:, :k].T
    threshold = q_threshold(eigval[k:])
    print(f"[eval] PCA: {k} components keep {VARIANCE:.0%} variance, SPE threshold {threshold:.3f}", flush=True)

    def spe(it):
        for pdf in it:
            y = (np.stack(pdf["x"].values) * idf - mu) @ residual
            yield pd.DataFrame({"block": pdf["block"], "anomaly": pdf["anomaly"], "warn_rule": pdf["warn_rule"],
                                "pca": (y * y).sum(1) > threshold})

    flagged = vec.mapInPandas(spe, "block string, anomaly boolean, warn_rule boolean, pca boolean").cache()

    rows = []
    for method in ("pca", "warn_rule"):
        rows.append(dict(method=method, blocks=n, templates=len(ids), **scores(flagged, method)))
    rows.append(dict(method="pca_published_he2016", blocks=575061, templates=29, tp="", fp="", fn="", **PUBLISHED_PCA))

    os.makedirs(RESULTS, exist_ok=True)
    out = os.path.join(RESULTS, "hdfs_accuracy.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    print(f"\n[eval] HDFS block-level accuracy ({time.time() - t0:.0f}s) -> {out}")
    print(f"{'method':<22}{'precision':>10}{'recall':>9}{'F1':>8}{'TP':>8}{'FP':>8}{'FN':>8}")
    for r in rows:
        print(f"{r['method']:<22}{r['precision']:>10}{r['recall']:>9}{r['f1']:>8}{r['tp']:>8}{r['fp']:>8}{r['fn']:>8}")
    spark.stop()


if __name__ == "__main__":
    main()
