#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash copy_datfiles_to_hdfs.sh <scale factor>${end}"
    exit 0
fi

#Copying the .dat files to hdfs.
printf "\n\n%s\n\n" "${blu}Copying to hdfs.${end}"
docker exec -ti  namenodecontainer  /bin/bash -c "hadoop fs -mkdir -p /temporal/$1GB && \
	hadoop fs -put /temporal/$1GB/* /temporal/$1GB" 

