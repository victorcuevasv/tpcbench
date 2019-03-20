#!/bin/bash   

#$1 Receives the command to execute as the first parameter (up, down, rm).

if [ $# -lt 1 ]; then
    echo "Usage: bash composeSpark.sh <up|down|rm>."
    exit 0
fi

USER_NAME=$(whoami)

USER_NAME_DC=$USER_NAME docker-compose -f docker-composeSpark.yml up

