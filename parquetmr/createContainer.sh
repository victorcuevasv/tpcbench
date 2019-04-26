#!/bin/bash

#Get the username the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build --force-rm --build-arg UNAME=$USER_NAME --build-arg UID=$USER_ID --build-arg GID=$GROUP_ID \
	-t parquetmr:dev $DIR -f $DIR/Dockerfile

