#!/bin/bash
#Execute the Java project with Maven by running the container (standalone container, no docker-compose). Can fail due to being unable to resolve localhost to the hiveservercontainer.

#docker run --rm -v $(pwd)/project:/project  --entrypoint mvn buildhiveclient:dev exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ProcessCreateScript" -Dexec.args="/data tpcds.sql" -f /project/pom.xml   

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

#docker exec -ti  hiveclientcontainer  /bin/bash -c "mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ProcessCreateScript\" -Dexec.args=\"/data tpcds.sql\" -f /project/pom.xml"

docker exec -ti  hiveclientcontainer  /bin/bash -c "mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" -Dexec.args=\"/data QueriesPostgresIterative results plans presto\" -f /project/pom.xml"

