#!/bin/bash

#Create a file with a table specifying the order of queries in the streams for the throughput test.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

docker run --rm --user $USER_ID:$GROUP_ID -v $DIR/client/project:/project -v $DIR/vols/data:/data \
	--entrypoint mvn clientbuilder:dev exec:java \
	 -Dexec.mainClass="org.bsc.dcc.vcv.ProcessStreamFilesQueries" \
	 -Dexec.args="/data Streams StreamsProcessed" -q -f /project/pomCreateScript.xml   

