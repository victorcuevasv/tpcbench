#!/bin/bash   

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 Receives the command to execute as the first parameter (up, down, rm).

if [ $# -lt 1 ]; then
    echo "Usage: bash composePresto.sh <up|down|rm>."
    exit 0
fi

USER_NAME=$(whoami)

USER_NAME_DC=$USER_NAME docker-compose -f $DIR/docker-compose.yml $1

