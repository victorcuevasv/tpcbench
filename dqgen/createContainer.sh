#!/bin/bash

#Receives as parameters the username, user id and group id of the user who is executing this script.
#
#$1 username
#$2 user id
#$3 group id

exitCode=0

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build --build-arg UNAME=$1 --build-arg UID=$2 --build-arg GID=$3 \
	-t tpcds:dev $DIR -f $DIR/Dockerfile

if [[ $? -ne 0 ]]; then
	exitCode=1
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi


