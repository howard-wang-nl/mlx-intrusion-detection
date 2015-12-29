#!/bin/bash
# spark-submit through ssh.

# Read configuration of variables.
. config.sh

ssh $USER@$HOST sudo -u spark $TDIR/submit.sh
