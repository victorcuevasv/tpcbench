#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Run the job on the databricks instance.
printf "\n\n%s\n\n" "${blu}Run a job using the REST API.${end}"

curl -X POST \
-H "Authorization: Bearer $(echo $DATABRICKS_TOKEN)" \
-H "Content-Type: application/json" \
-d @jobExample.json \
https://dbc-08fc9045-faef.cloud.databricks.com/api/2.0/jobs/create


