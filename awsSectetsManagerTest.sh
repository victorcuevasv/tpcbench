#!/bin/bash

#Create a file with a table specifying the order of queries in the streams for the throughput test.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash awsSecretsManagerTest.sh <secret name>${end}"
    exit 0
fi

docker run --rm --user $USER_ID:$GROUP_ID -v $DIR/client/project:/project -v $DIR/vols/data:/data \
	--entrypoint mvn clientbuilder:dev exec:java \
	 -Dexec.mainClass="org.bsc.dcc.vcv.GetSecretValue" \
	 -Dexec.args="$1" -q -f /project/pom.xml   

