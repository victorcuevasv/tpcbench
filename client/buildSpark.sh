#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t buildsparkhiveclient:dev $DIR -f $DIR/DockerfileSpark

