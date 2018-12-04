#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build -t hiveserver:dev $DIR
docker run --name hivecontainer hiveserver:dev 
docker cp hivecontainer:/metastore_db ../metastorevol
docker stop hivecontainer
docker rm hivecontainer
