#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t sparkmaster:dev $DIR -f $DIR/DockerfileSparkMaster 

