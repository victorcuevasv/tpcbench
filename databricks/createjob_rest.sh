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
    echo "${yel}Usage: bash createjob_rest.sh <json file>.${end}"
    exit 0
fi

#Run the job on the databricks instance.
printf "\n\n%s\n\n" "${blu}Run a job using the REST API.${end}"

curl -X POST \
-H "Authorization: Bearer $(echo $DATABRICKS_TOKEN)" \
-H "Content-Type: application/json" \
-d @$1 \
https://dbc-08fc9045-faef.cloud.databricks.com/api/2.0/jobs/create


