#!/bin/bash
#Execute the Java project with Maven by running the container (standalone container, no docker-compose).
#Receives as parameters the user and group id of the user who is executing this script.
#
#$1 user id
#$2 group id

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker run --rm --user $1:$2 -v $DIR/client/project:/project -v $DIR/vols/data:/data \
	--entrypoint mvn clientbuilder:dev exec:java \
	 -Dexec.mainClass="org.bsc.dcc.vcv.ProcessCreateScript" \
	 -Dexec.args="/data tables tpcds.sql" -f /project/pom.xml   

