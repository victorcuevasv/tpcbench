#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build --build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3 \
	-t sparkhiveserver:dev $DIR -f $DIR/DockerfileSparkMaster 

