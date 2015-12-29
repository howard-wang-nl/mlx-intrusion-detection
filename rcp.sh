#!/bin/bash
# Copy assembly jar file to remote host through scp and prepare files for spark-submit through ssh.

# Extract full path of the assembly jar file to copy.
#JAR_FP=`sbt -Dsbt.log.noformat=true "show assembly::assemblyOutputPath" |grep "\.jar$" |sed "s/^\[info] //"`
JAR_FP=`sbt -no-colors "show assembly::assemblyOutputPath" |grep "\.jar$" |sed "s/^\[info] //"`

# File name without path prefix.
JAR_FILE=${JAR_FP##*/}

# Read configuration variables.
. config.sh

cat > submit.sh <<EOF
#!/bin/bash
# Submit assembly jar to Spark Yarn Cluster.
# This script is designed to be run on one of the Spark driver hosts through ssh.

cd "$TDIR"
$SPARK_SUBMIT_CMD
EOF

# Create target directory.
ssh $USER@$HOST <<EOF
if [ ! -e "$TDIR" ]
then
  mkdir -p -m "go-w" "$TDIR"
fi
EOF

# Copy files.
scp $JAR_FP submit.sh $USER@$HOST:"$TDIR"

# Correct file permissions.
ssh $USER@$HOST <<EOF
chmod a+x "$TDIR"/*.sh
chmod a+r "$TDIR"/*
EOF
