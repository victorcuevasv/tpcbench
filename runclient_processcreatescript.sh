#!/bin/bash
#Execute the Java project with Maven by running the container (standalone container, no docker-compose). Can fail due to being unable to resolve localhost to the hiveservercontainer.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker run --rm -v $DIR/client/project:/project -v $DIR/datavol:/data  --entrypoint mvn clientbuilder:dev exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ProcessCreateScript" -Dexec.args="/data tables tpcds.sql" -f /project/pom.xml   


