#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Copying the database to hdfs.
printf "\n\n%s\n\n" "${blu}Copying to hdfs.${end}"
docker exec -ti mastercontainer  /bin/bash -c \
	"hadoop fs -put /user/hive/warehouse/* /user/hive/warehouse" 


