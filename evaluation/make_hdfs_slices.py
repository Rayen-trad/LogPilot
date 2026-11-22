#!/usr/bin/env python3
"""Split the start of HDFS.log into a normal slice and an anomaly slice using the block labels.

    python evaluation/make_hdfs_slices.py --lines 500000
    -> data/loghub/HDFS_v1/slices/normal.log   lines whose blocks are all labelled Normal
       data/loghub/HDFS_v1/slices/anomaly.log  lines touching at least one Anomaly block

Replay normal.log steadily to build baselines, then inject anomaly.log to test detection.
"""
import argparse
import csv
import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..", "data", "loghub", "HDFS_v1")
BLK = re.compile(r"blk_-?\d+")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=int, default=500_000, help="lines to read from the start of HDFS.log")
    args = ap.parse_args()

    with open(os.path.join(ROOT, "preprocessed", "anomaly_label.csv")) as f:
        anomalous = {row["BlockId"] for row in csv.DictReader(f) if row["Label"] == "Anomaly"}

    out_dir = os.path.join(ROOT, "slices")
    os.makedirs(out_dir, exist_ok=True)
    n_norm = n_anom = 0
    with open(os.path.join(ROOT, "HDFS.log"), encoding="utf-8", errors="replace") as src, \
            open(os.path.join(out_dir, "normal.log"), "w", encoding="utf-8", newline="\n") as norm, \
            open(os.path.join(out_dir, "anomaly.log"), "w", encoding="utf-8", newline="\n") as anom:
        for i, line in enumerate(src):
            if i >= args.lines:
                break
            if any(b in anomalous for b in BLK.findall(line)):
                anom.write(line)
                n_anom += 1
            else:
                norm.write(line)
                n_norm += 1
    print(f"{n_norm:,} normal lines, {n_anom:,} anomaly lines -> {os.path.abspath(out_dir)}")


if __name__ == "__main__":
    main()
