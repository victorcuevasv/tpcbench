#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t hiveserver:dev $DIR -f $DIR/DockerfileStandaloneExternalMetastore 

