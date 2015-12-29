#!/bin/bash
# spark-submit through ssh.

# Read configuration of variables.
. config.sh

ssh $USER@$HOST sudo -u $SPARK_USER $TDIR/submit.sh
