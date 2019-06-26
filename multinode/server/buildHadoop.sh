#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

exitCode=0

#Check if the local mirror can be used.
nc -z localhost 8888 && nc -z localhost 443
mirror=$?
if [[ $mirror -eq 0 ]]; then
	docker build --network="host" --force-rm -t ubuntujavahadoop:dev $DIR -f $DIR/DockerfileHadoop \
	--build-arg APACHE_MIRROR=localhost:8888
else
	docker build --force-rm -t ubuntujavahadoop:dev $DIR -f $DIR/DockerfileHadoop
fi

if [[ $? -ne 0 ]]; then
	exitCode=1
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi


