#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Receives as parameters the user and group id of the user who is executing this script.
#$1 user id
#$2 group id
#$3 scale

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
TPCDS_VERSION=v2.13.0rc1

#Build the client project.
printf "\n\n%s\n\n" "${blu}Generating queries.${end}"

docker run --rm --user $1:$2 --name tpc --volume $DIR/../vols/data:/TPC-DS/$TPCDS_VERSION/output \
	tpcds:dev /TPC-DS/$TPCDS_VERSION/tools/createQueries.sh $3       

