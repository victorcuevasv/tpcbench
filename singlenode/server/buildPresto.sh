#!/bin/bash

#Receives as parameters the user name, user id, and group id of the user who is executing this script.
#
#$1 user name
#$2 user id
#$3 group id

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Used to indicate to a parent script if the build command was successful.
exitCode=0

#Check if the local mirror can be used.
nc -z localhost 8888 && nc -z localhost 443
mirror=$?
if [[ $mirror -eq 0 ]]; then
	docker build --network="host" -t prestohiveserver:dev $DIR -f $DIR/DockerfilePresto \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg POSTGRES_DRIVER_MIRROR=localhost:443 \
	--build-arg PRESTO_MIRROR=localhost:443 \
	--build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3
else
	docker build -t prestohiveserver:dev $DIR -f $DIR/DockerfilePresto \
	--build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3
fi

if [[ $? -ne 0 ]]; then
	exitCode=1;
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi

