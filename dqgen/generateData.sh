#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Generate the data.
printf "\n\n%s\n\n" "${blu}Generating the data.${end}"

mkdir $DIR/../hivevol/$1GB

docker run --rm --name tpc --volume $DIR/../hivevol:/TPC-DS/v2.10.1rc3/output \
	--entrypoint /TPC-DS/v2.10.1rc3/tools/dsdgen tpcds:dev \
	-scale $1 -dir ../output/$1GB  -terminate n   

