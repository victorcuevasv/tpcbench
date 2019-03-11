#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Build the project with the container using Maven by running the container (standalone, with no docker-compose).
docker run --rm -v $DIR/project:/project  \
--entrypoint mvn  clientbuilder:dev clean package -f /project/pom.xml   

