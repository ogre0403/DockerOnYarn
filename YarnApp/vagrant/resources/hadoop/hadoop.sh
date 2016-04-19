#!/bin/sh

export HADOOP_PREFIX=/opt/hadoop
export HADOOP_YARN_HOME=${HADOOP_PREFIX}
export HADOOP_CONF_DIR=${HADOOP_PREFIX}/etc/hadoop
export YARN_LOG_DIR=${HADOOP_YARN_HOME}/logs
export YARN_IDENT_STRING=root
export HADOOP_MAPRED_IDENT_STRING=root
export HADOOP_MAPRED_HOME=${HADOOP_PREFIX}
export PATH=${HADOOP_PREFIX}/bin:${HADOOP_PREFIX}/sbin:${PATH}

export JAVA_HOME=/opt/java
export PATH=${JAVA_HOME}/bin:${PATH}