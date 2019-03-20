#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

exitCode=0

#Use the default mirrors.
#docker build -t ubuntujavahadoop:dev $DIR -f $DIR/DockerfileHadoop

#Use the apache mirror server.
docker build --network="host" -t ubuntujavahadoop:dev $DIR -f $DIR/DockerfileHadoop \
	--build-arg APACHE_MIRROR=localhost:8888 

if [[ $? -ne 0 ]]; then
	exitCode=1
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi


