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
#$2 user id
#$3 group id
#$4 degree of parallelism

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
TPCDS_VERSION=v2.13.0rc1

#Generate the data.
printf "\n%s\n\n" "${cyn}Generating the data with parallelism.${end}"

mkdir $DIR/../vols/hive/$1GB

#Get the time of start of execution to measure total execution time.
start_time=$(date +%s)

#Use the (c octal) character to separate columns.
docker run --rm --user $2:$3 --name tpc --volume $DIR/../vols/hive:/TPC-DS/$TPCDS_VERSION/output \
	--entrypoint "/TPC-DS/$TPCDS_VERSION/tools/runParallelCmd.sh" tpcds:dev \
	$4 $1 ../output/$1GB $(echo -e "\001")

end_time=$(date +%s)

runtime=$((end_time-start_time))
printf "\n%s\n\n" "${cyn}Total execution time: ${runtime} sec.${end}\n"



