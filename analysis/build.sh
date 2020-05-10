#!/bin/bash

AWS_ACCESS_KEY=""
AWS_SECRET_KEY=""

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

if [ ${#AWS_ACCESS_KEY} -eq 0 ] && [ ${#AWS_SECRET_KEY} -eq 0 ]; then
    echo "${yel}Error: the AWS_ACCESS_KEY and AWS_SECRET_KEY variables have not been set in this script.${end}"
    exit 0
fi

docker build -t rstudio:dev $DIR -f $DIR/Dockerfile \
--build-arg AWS_ACCESS_KEY=$AWS_ACCESS_KEY \
--build-arg AWS_SECRET_KEY=$AWS_SECRET_KEY

