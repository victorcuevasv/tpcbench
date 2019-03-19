#!/bin/bash

#Receives as parameters the user name, user id, and group id of the user who is executing this script.
#
#$1 user name
#$2 user id
#$3 group id

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Use the default mirrors.
#docker build -t sparkhiveservermult:dev $DIR -f $DIR/DockerfileSparkMaster \
#	--build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3

#Use the nginx mirror server.
docker build --network="host" -t sparkhiveservermult:dev $DIR -f $DIR/DockerfileSparkMaster  \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg POSTGRES_DRIVER_MIRROR=localhost:443 \
	--build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3

