#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Execute the Java project with Maven by running the container (standalone container, no docker-compose).
#Receives as parameters the user and group id of the user who is executing this script.
#
#$1 user id
#$2 group id
#$3 create table statements sql file

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Create and populate the database from the .dat files. The scale factor is passed as an argument
#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_processcreatescript.sh <user id> <group id> <create tables sql file>.${end}"
    exit 0
fi

docker run --rm --user $1:$2 -v $DIR/client/project:/project -v $DIR/vols/data:/data \
	--entrypoint mvn clientbuilder:dev exec:java \
	 -Dexec.mainClass="org.bsc.dcc.vcv.ProcessCreateScript" \
	 -Dexec.args="/data tables $3" -q -f /project/pomCreateScript.xml   

