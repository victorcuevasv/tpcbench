#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#The scale factor is passed as an argument.
#Also receives as parameters the user and group id of the user who is executing this script.
#$1 scale factor (positive integer)

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Generate the data.
printf "\n\n%s\n\n" "${blu}Generating the data.${end}"

sudo mkdir -p /mnt/vols/hive/$1GB

sudo chmod -R 777 /mnt/vols/hive/$1GB

docker run --rm --user $2:$3 --name tpc --volume /mnt/vols/hive:/TPC-DS/v2.10.1rc3/output \
	--entrypoint /TPC-DS/v2.10.1rc3/tools/dsdgen tpcds:dev \
	-scale $1 -dir ../output/$1GB  -terminate n -delimiter $(echo -e "\001")

sudo ln -s /mnt/vols/hive/$1GB $DIR/../vols/hive/$1GB



