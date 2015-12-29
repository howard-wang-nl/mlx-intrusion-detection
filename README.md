# mlx-intrusion-detection
K-means clustering analysis on KDD Cup 1999 data set for network intrusion detection.

## Compilation and packaging

Run `sbt assembly`.

## Submission to cluster

1. Edit config.sh to reflect your choice of settings on remote host.
2. Run `rcp.sh` to copy files to remote host.  Only updated files will be copied using rsync.
3. Run `rrun.sh` to submit task on remote host.
