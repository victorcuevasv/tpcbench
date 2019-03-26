#!/bin/bash

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#docker exec --user $USER_ID:$GROUP_ID -it namenodecontainer bash -c \
#	"sudo -E -u $USER_NAME beeline -u jdbc:hive2://localhost:10000/default"
	
docker exec -it namenodecontainer bash -c \
	"beeline -u jdbc:hive2://localhost:10000/default"

