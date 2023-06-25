#!/bin/sh
runningApp="controller"
#cd $(dirname $0)
BIN_DIR=$(pwd)
#cd ..
export ROCKETMQ_GO_LOG_LEVEL=error
SERVER_HOME="/opt/mtoss/controller"
echo "nohup $SERVER_HOME/bin/$runningApp  > $SERVER_HOME/logs/stdout.out & "
nohup ./bin/controller start > ./logs/stdout.out 2>&1 < /dev/null
