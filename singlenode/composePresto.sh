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

#$1 Receives the command to execute as the first parameter (up, down, rm).

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash composePresto.sh <up | down | rm>${end}"
    exit 0
fi

USER_NAME=$(whoami)

USER_NAME_DC=$USER_NAME docker-compose -f $DIR/docker-composePresto.yml $1

