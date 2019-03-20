#!/bin/bash   

#$1 Receives the command to execute as the first parameter (up, down, rm).

if [ $# -lt 1 ]; then
    echo "Usage: bash composePresto.sh <up|down|rm>."
    exit 0
fi

docker-compose -f docker-compose.yml $1

