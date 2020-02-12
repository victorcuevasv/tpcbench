#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 job id (positive integer)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash getjob_rest.sh <job id>${end}"
    exit 0
fi

#Run the job on the databricks instance.
printf "\n\n%s\n\n" "${blu}Retrieving job information through the REST API.${end}"

curl -X GET \
-H "Authorization: Bearer $(echo $DATABRICKS_TOKEN)" \
https://dbc-08fc9045-faef.cloud.databricks.com/api/2.0/jobs/get?job_id=$1


