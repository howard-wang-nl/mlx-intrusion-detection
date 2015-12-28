#!/usr/bin/env bash
spark-submit --master yarn-cluster \
  --driver-memory 4g \
  --executor-memory 4g \
  ~admin/hw/spark/mlx-intrusion-detection-assembly-0.1.0.jar -k 10 --relDifWSSSE 0.01 /playground/kdd/kddcup.data_10_percent
