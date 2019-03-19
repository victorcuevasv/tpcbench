#!/bin/bash

#Receives as parameters the user name, user id, and group id of the user who is executing this script.
#
#$1 user name
#$2 user id
#$3 group id

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build --build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3 \
	-t sparkhiveserver:dev $DIR -f $DIR/DockerfileSparkMaster 

