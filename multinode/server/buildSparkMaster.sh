#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t sparkhiveservermult:dev $DIR -f $DIR/DockerfileSparkMaster  --build-arg APACHE_MIRROR=apache.rediris.es       

