#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "Usage: bash copy_datfiles_to_S3.sh <scale factor>."
    exit 0
fi

#Get the time of start of execution to measure total execution time.
start_time=$(date +%s)

#Copying the .dat files to hdfs.
printf "\n\n%s\n\n" "${blu}Copying to S3.${end}"
aws s3 sync $DIR/../vols/hive/$1GB s3://tpcds-datasets/$1GB

end_time=$(date +%s)

runtime=$((end_time-start_time))
printf "\n%s\n\n" "${cyn}Total execution time: ${runtime} sec.${end}"


