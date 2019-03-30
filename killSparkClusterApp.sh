#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 driver id

if [ $# -lt 1 ]; then
    echo "Usage bash killSparkCluster.sh <driver id>."
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Killing the application.${end}"
docker exec -ti  namenodecontainer  /bin/bash -c \
	"spark-class org.apache.spark.deploy.Client kill spark://namenodecontainer:7077 $1" 



