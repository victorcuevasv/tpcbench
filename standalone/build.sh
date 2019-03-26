#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Check if the local mirror can be used.
nc -z localhost 8888 && nc -z localhost 443
mirror=$?
if [[ $mirror -eq 0 ]]; then
	docker build --network="host" -t hiveserverstandalone:dev $DIR/server -f $DIR/server/DockerfileStandalone \
	--build-arg APACHE_MIRROR=localhost:8888
else
	docker build --network="host" -t hiveserverstandalone:dev $DIR/server -f $DIR/server/DockerfileStandalone
fi


