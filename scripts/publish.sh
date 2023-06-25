#!/bin/bash
#

WORKSPACE=$1
PROJECT_NAME=$2
BUILD_DIR=".build/$PROJECT_NAME"

echo $WORKSPACE
echo $PROJECT_NAME
echo $BUILD_DIR

if [ ! -d "$BUILD_DIR" ];then
   mkdir -p "$BUILD_DIR"
fi
LOGS="$BUILD_DIR/logs"
if [ ! -d "${LOGS}" ];then
   mkdir -p "${LOGS}"
fi
BIN="$BUILD_DIR/bin"
if [ ! -d "$BIN" ];then
   mkdir -p "$BIN"
fi

BIN="$BUILD_DIR/conf"
if [ ! -d "$BIN" ];then
   mkdir -p "$BIN"
fi


cp "$WORKSPACE/.build/bin/$PROJECT_NAME" "$BUILD_DIR/bin"
cp  "$WORKSPACE"/conf/$PROJECT_NAME.yml $BUILD_DIR/conf
#cp -r   "$WORKSPACE"/cmd/$PROJECT_NAME/cert $BUILD_DIR
cp  "$WORKSPACE"/docker/$PROJECT_NAME/docker-startup.sh $BUILD_DIR

echo "copying template"

echo "release step all finshed"
