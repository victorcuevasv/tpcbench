#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Check if the local mirror can be used.
nc -z localhost 8888 && nc -z localhost 443
mirror=$?
if [[ $mirror -eq 0 ]]; then
	docker build --network="host" -t hiveserverstandalone:dev $DIR/server -f $DIR/server/DockerfileStandalone \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg UNAME=$USER_NAME --build-arg UID=$USER_ID --build-arg GID=$GROUP_ID
else
	docker build --network="host" -t hiveserverstandalone:dev $DIR/server -f $DIR/server/DockerfileStandalone \
	--build-arg UNAME=$USER_NAME --build-arg UID=$USER_ID --build-arg GID=$GROUP_ID
fi


