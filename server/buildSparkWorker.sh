#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

exitCode=0

docker build --network="host" -t sparkworker:dev $DIR -f $DIR/DockerfileSparkWorker \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg POSTGRES_DRIVER_MIRROR=localhost:443 \
	--build-arg PRESTO_MIRROR=localhost:443

if [[ $? -ne 0 ]]; then
	exitCode=1;
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi

