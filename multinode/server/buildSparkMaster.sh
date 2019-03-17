#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Use the default mirrors.
#docker build -t sparkhiveservermult:dev $DIR -f $DIR/DockerfileSparkMaster

#Use the nginx mirror server.
docker build --network="host" -t sparkhiveservermult:dev $DIR -f $DIR/DockerfileSparkMaster  \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg POSTGRES_DRIVER_MIRROR=localhost:8888

