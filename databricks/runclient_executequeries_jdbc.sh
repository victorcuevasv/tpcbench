#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_executequeries_jdbc.sh <scale factor> <all | query filename>${end}"
    exit 0
fi

mvn exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ExecuteQueries" \
	-Dexec.args="data QueriesSpark results plans sparkdatabricksjdbc $(hostname) true true tpcdsdb$1gb $2" \
	-f $DIR/../client/project/pom.xml      

