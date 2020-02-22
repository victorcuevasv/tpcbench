#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Copying the file to the databricks instance.
printf "\n\n%s\n\n" "${blu}Uploading the jar using REST API.${end}"

curl -n \
-H "Authorization: Bearer $(echo $DATABRICKS_TOKEN)" \
-F filedata=@"$DIR/../client/project/targetsparkdatabricks/client-1.1-SNAPSHOT-jar-with-dependencies.jar" \
-F path="/FileStore/job-jars/project/targetsparkdatabricks/client-1.1-SNAPSHOT-jar-with-dependencies.jar" \
-F overwrite=true \
https://dbc-08fc9045-faef.cloud.databricks.com/api/2.0/dbfs/put


