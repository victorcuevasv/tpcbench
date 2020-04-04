#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 s3 bucket to mount
#$2 s3 prefix for experiment files
#$3 script to execute


if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash run_script.sh <bucket to mount> <s3 prefix> <script to run>${end}"
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker run --rm --privileged \
-v $DIR/Documents:/home/rstudio/Documents  \
-v $DIR/Output:/home/rstudio/Output  \
--entrypoint /bin/bash rstudio:dev -c \
"mkdir -p /home/rstudio/s3buckets/$1; s3fs $1 /home/rstudio/s3buckets/$1; Rscript /home/rstudio/Documents/$3 $1 $2"        
 
