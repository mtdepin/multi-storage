#!/bin/sh
runningApp="chunker"
#cd $(dirname $0)
BIN_DIR=$(pwd)
#cd ..
export ROCKETMQ_GO_LOG_LEVEL=error
SERVER_HOME="/opt/mtoss/chunker"
echo "nohup $SERVER_HOME/bin/$runningApp  > $SERVER_HOME/logs/stdout.out & "
nohup ./bin/chunker start > ./logs/stdout.out 2>&1 < /dev/null
