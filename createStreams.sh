#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#PARAMETERS.
#$1 scale factor (positive integer)
#$2 number of streams

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash createStreams.sh <scale factor> <number of streams>${end}"
    exit 1
fi

#Generate the Netezza queries.
printf "\n\n%s\n\n" "${mag}Generating query streams.${end}"
bash $DIR/dqgen2/generateStreams.sh $USER_ID $GROUP_ID $1 $2
 


