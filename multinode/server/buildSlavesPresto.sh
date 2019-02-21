#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t sparkslave:dev $DIR -f $DIR/DockerfileSlavePresto --build-arg APACHE_MIRROR=apache.uvigo.es 

