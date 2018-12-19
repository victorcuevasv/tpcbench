#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Build the project with the container using Maven by running the container (standalone, with no docker-compose).
docker run --rm -v $DIR/project:/project  \
--entrypoint mvn  buildsparkhiveclient:dev clean package -f /project/pom.xml   

#If the buildhiveclient container is running in the docker compose.
#docker exec -ti  hiveclientcontainer  /bin/bash -c "mvn clean package -f /project/pom.xml"

