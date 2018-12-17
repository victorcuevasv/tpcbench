#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t sparkhiveserver:dev $DIR -f $DIR/DockerfileSparkMaster 

