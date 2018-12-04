#!/bin/bash
#Build the project with the container using Maven by running the container (standalone, with no docker-compose).
docker run --rm -v $(pwd)/project:/project  --entrypoint mvn  buildhiveclient:dev clean package -f /project/pom.xml   

#If the buildhiveclient container is running in the docker compose.
#docker exec -ti  hiveclientcontainer  /bin/bash -c "mvn clean package -f /project/pom.xml"
