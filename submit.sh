#!/bin/bash
# Submit assembly jar to Spark Yarn Cluster.
# This script is designed to be run on one of the Spark driver hosts through ssh.

cd "/tmp/hw"

spark-submit --master yarn-cluster   --driver-memory 4g   --executor-memory 4g   mlx-intrusion-detection-assembly-0.1.0.jar -k 10 --relDifWSSSE 0.01 /playground/kdd/kddcup.data_10_percent

