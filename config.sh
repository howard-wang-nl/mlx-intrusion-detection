# Remote host to copy the assembly jar file to and run spark-submit on.
HOST=lab5

# Remote SSH user.
USER=admin

# Base directory to save the files on remote host.
BASE_DIR=/tmp

# Subdirectory under BASE_DIR to save task related files.
FILES_DIR=hw

# Target directory on remote host.
TDIR="$BASE_DIR/$FILES_DIR"

JAR_FILE=${JAR_FILE:-assembly.jar}

# JAR_FILE should be defined by the caller script.
SPARK_SUBMIT_CMD="
spark-submit --master yarn-cluster \
  --driver-memory 4g \
  --executor-memory 4g \
  $JAR_FILE -k 10 --relDifWSSSE 0.01 /playground/kdd/kddcup.data_10_percent
"
