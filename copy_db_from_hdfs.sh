#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Copying the database from hdfs.
printf "\n\n%s\n\n" "${blu}Copying from hdfs.${end}"
docker exec --user $USER_ID:$GROUP_ID -ti  mastercontainer  /bin/bash -c \
	"hadoop fs -get /user/hive/warehouse/* /user/hive/warehouse" 


