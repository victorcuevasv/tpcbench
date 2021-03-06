#!/bin/bash

#Execute bash on the container provided as a parameter. 

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

if [ $# -lt 1 ]; then
    echo "${yel}Usage bash containerexec_user.sh <container name>.${end}"
    exit 0
fi

docker exec --user $USER_ID:$GROUP_ID -ti $1 bash 

