#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

BucketToMount="tpcds-results-test"
ScriptToRun="stacked_chart.R"

#Example:
#bash run_script.sh tpcds-results-test presto-comp/analytics/ stacked_chart.R experiments.txt

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash run_script.sh <arguments for the script>${end}"
    echo "${yel}Bucket to be mounted: ${BucketToMount}${end}"
    echo "${yel}R script to be executed: ${ScriptToRun}${end}"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker run --rm --privileged \
-v $DIR/Documents:/home/rstudio/Documents  \
-v $DIR/Output:/home/rstudio/Output  \
--entrypoint /bin/bash rstudio:dev -c \
"mkdir -p /home/rstudio/s3buckets/${BucketToMount}; s3fs ${BucketToMount} /home/rstudio/s3buckets/${BucketToMount}; Rscript /home/rstudio/Documents/${ScriptToRun} $@"        
 
