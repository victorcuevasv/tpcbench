#!/bin/bash
#Receives as parameters the user and group id of the user who is executing this script.
#
#$1 user id
#$2 group id

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Build the project with the container using Maven by running the container (standalone, with no docker-compose).
docker run --rm --user $1:$2 -v $DIR/project:/project  \
--entrypoint mvn clientbuilder:dev -q clean package -f /project/pomSparkCluster.xml   


