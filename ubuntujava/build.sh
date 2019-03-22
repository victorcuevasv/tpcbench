#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

exitCode=0

docker build -t ubuntujava:dev $DIR

if [[ $? -ne 0 ]]; then
	exitCode=1
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi

