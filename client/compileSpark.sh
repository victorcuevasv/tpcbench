#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Build the project with the container using Maven by running the container (standalone, with no docker-compose).
docker run --rm -v $DIR/project:/project  \
--entrypoint mvn  buildsparkhiveclient:dev clean package -f /project/pomSpark.xml   

#If the sparkhiveclientcontainer container is running in the docker compose.
#docker exec -ti  sparkhiveclientcontainer  /bin/bash -c "mvn clean package -q -f /project/pomSpark.xml"

