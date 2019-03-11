#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Build the client project.
printf "\n\n%s\n\n" "${blu}Generating queries.${end}"

docker run --rm --name tpc --volume $DIR/output:/TPC-DS/v2.10.1rc3/output \
	tpcds:dev /TPC-DS/v2.10.1rc3/tools/createQueries.sh        

