#!/bin/bash
#Execute the Java project with Maven by running the container (standalone container, no docker-compose).

#docker run --rm -v $(pwd)/project:/project  --entrypoint mvn buildhiveclient:dev exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ProcessCreateScript" -Dexec.args="/project/data tpcds.sql" -f /project/pom.xml   

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

#docker exec -ti  hiveclientcontainer  /bin/bash -c "mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ProcessCreateScript\" -Dexec.args=\"/project/data tpcds.sql\" -f /project/pom.xml"

docker exec -ti  hiveclientcontainer  /bin/bash -c "mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.CreateDatabase\" -Dexec.args=\"/project/data/tables\" -f /project/pom.xml"
