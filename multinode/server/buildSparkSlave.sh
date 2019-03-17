#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t sparkslavemult:dev $DIR -f $DIR/DockerfileSparkSlave 

