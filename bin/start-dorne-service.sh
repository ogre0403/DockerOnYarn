#!/bin/bash

if [ ! -f "$(dirname $0)/dorne-env.sh" ]; then
    echo "dorne-env.sh not exist !!"
    exit 1
fi

. "$(dirname $0)/dorne-env.sh"

if [ $# -ne 1 ]; then
    echo "Usgae: ./start-dorne-service.sh <yaml_file>"
    exit 1;
fi


$JAVA -cp $CLASSPATH dorne.Client --jar ${DORNE_JAR} --yaml $1
