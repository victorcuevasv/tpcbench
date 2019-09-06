#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_create_schema_spark.sh <scale factor> <experiment instance number>${end}"
    exit 0
fi

spark-submit --conf spark.eventLog.enabled=true  \
--class org.bsc.dcc.vcv.CreateSchemaSpark \
--master yarn --deploy-mode cluster \
/mnt/efs/FileStore/job-jars/project/targetsparkdatabricks/client-1.0-SNAPSHOT-jar-with-dependencies.jar \
spark tpcdsdb$1gb_$2


