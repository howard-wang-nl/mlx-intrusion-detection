#!/usr/bin/env bash
sbt assembly &&
spark-submit --master 'local[*]' \
  --driver-memory 4g \
  --executor-memory 4g \
  /Users/hwang/IdeaProjects/mlx-intrusion-detection/target/scala-2.10/mlx-intrusion-detection-assembly-0.1.0.jar -k 10 data/kddcup.data_10_percent.csv
