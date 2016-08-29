#!/bin/bash

if [ ! -f "$(dirname $0)/dorne-env.sh" ]; then
    echo "dorne-env.sh not exist !!"
    exit 1
fi

. "$(dirname $0)/dorne-env.sh"

if [ $# -ne 2 ]; then
    echo "Usage: ./shutdown-dorne-service.sh <server> <port>."
    exit 1;
fi

$JAVA -cp $CLASSPATH dorne.thrift.ThriftClient --server $1 --port $2 --operation shutdown
