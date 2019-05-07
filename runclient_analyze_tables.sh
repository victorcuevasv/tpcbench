#!/bin/bash

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#docker exec -ti --user $USER_ID:$GROUP_ID clientbuildercontainer  /bin/bash -c \
#	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.AnalyzeTables\" \
#	-Dexec.args=\"hive namenodecontainer false\" \
#	-f /project/pom.xml"    
	
docker exec -ti --user $USER_ID:$GROUP_ID clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.AnalyzeTables\" \
	-Dexec.args=\"spark namenodecontainer false\" \
	-f /project/pomSparkJDBC.xml"  
   

