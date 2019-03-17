#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Use the default mirrors.
#docker build -t sparkslavemult:dev $DIR -f $DIR/DockerfileSparkSlave 

#Use the nginx mirror server.
docker build --network="host" -t sparkslavemult:dev $DIR -f $DIR/DockerfileSparkSlave \
	--build-arg APACHE_MIRROR=localhost:8888 \

	