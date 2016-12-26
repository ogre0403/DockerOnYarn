#!/bin/bash

export DORNE_HOME=$(dirname $0)/..
export DORNE_LIB_PATH=${DORNE_HOME}/lib
export DORNE_JAR=${DORNE_LIB_PATH}/`basename ${DORNE_LIB_PATH}/dorne-*`
export JERSEY_LIB=${DORNE_LIB_PATH}/jersey
export DEPEND_LIB=${DORNE_LIB_PATH}/dependency

export CLASSPATH=$DORNE_JAR:${DEPEND_LIB}/*:${JERSEY_LIB}/jersey-client-1.9.jar:${JERSEY_LIB}/jersey-core-1.9.jar:$HADOOP_CONF_DIR
export JAVA=`which java`