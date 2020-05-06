#!/bin/bash

#Receives as parameters the username, user id and group id of the user who is executing this script.
#A final parameter indicates whether to use the local mirror server for dependencies.

#$1 username
#$2 user id
#$3 group id
#$4 use local mirror (1/0)
#$5 build jdbc client (true/false)

exitCode=0

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Check if the local mirror can be used.
nc -z localhost 8888 && nc -z localhost 443 && [ $4 -eq 1 ]
mirror=$?
if [[ $mirror -eq 0 ]]; then
	docker build --network="host" --force-rm -t clientbuilder:dev $DIR -f $DIR/DockerfileSingle \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg PRESTO_MIRROR=localhost:443 \
	--build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3
else
	docker build --network="host" --force-rm -t clientbuilder:dev $DIR -f $DIR/DockerfileSingle \
	--build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3 --build-arg JDBC=$5
fi

if [[ $? -ne 0 ]]; then
	exitCode=1
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi


