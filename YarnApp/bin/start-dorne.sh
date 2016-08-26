#!/usr/bin/env bash

export DORNE_HOME=/home/hadoop
export DORNE_LIB=/home/hadoop/dorne_lib
export CLASSPATH=$DORNE_LIB/*:/home/hadoop/jersey-client-1.9.jar:/home/hadoop/jersey-core-1.9.jar:/opt/hadoop/etc/hadoop

java -cp ./dorne-1.0-SNAPSHOT.jar:$CLASSPATH dorne.Client --jar dorne-1.0-SNAPSHOT.jar --yaml mpi.yml
