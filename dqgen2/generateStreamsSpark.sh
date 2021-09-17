#!/bin/bash
set -euxo pipefail
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
#$4 number of streams

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#The globalVars.sh script defines the TPCDS_VERSION variable.
source $DIR/globalVars.sh

printf "\n\n%s\n\n" "${blu}Generating the Spark query stream files.${end}"

docker run --rm --user $1:$2 --name tpc --volume $DIR/../vols/data:/TPC-DS/$TPCDS_VERSION/output \
	tpcds:dev /TPC-DS/$TPCDS_VERSION/tools/createStreamsSpark.sh $3 $4    
	

