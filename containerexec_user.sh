#!/bin/bash

#Execute bash on the container provided as a parameter. 

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

if [ $# -lt 1 ]; then
    echo "Usage bash containerexec_user.sh <container name>."
    exit 0
fi

docker exec --user $USER_ID:$GROUP_ID -ti $1 bash 

