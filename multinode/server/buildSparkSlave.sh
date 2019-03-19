#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

exitCode=0

#Use the default mirrors.
#docker build -t sparkslavemult:dev $DIR -f $DIR/DockerfileSparkSlave 

#Use the nginx mirror server.
docker build --network="host" -t sparkslavemult:dev $DIR -f $DIR/DockerfileSparkSlave \
	--build-arg APACHE_MIRROR=localhost:8888 

if [[ $? -ne 0 ]]; then
	exitCode=1
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi

