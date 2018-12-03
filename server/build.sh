#!/bin/bash
docker build -t hiveserver:dev .
docker run --name hivecontainer hiveserver:dev 
docker cp hivecontainer:/metastore_db ../metastorevol
docker stop hivecontainer
docker rm hivecontainer
