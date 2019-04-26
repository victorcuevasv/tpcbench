#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 Name of the file (must be inside the input directory)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash getFileMetadata.sh <file inside input dir>${end}"
    exit 0
fi

#Obtain the metadata.
printf "\n%s\n\n" "${cyn}Obtaining the metadata.${end}"

docker run --rm --name parquetmr --volume $DIR/input:/input \
	--entrypoint java parquetmr:dev -jar /opt/parquet-mr/parquet-tools/target/parquet-tools-1.12.0-SNAPSHOT.jar \
	meta /input/$1

#docker run -it --rm --name parquetmr --volume $DIR/input:/input \
#	--entrypoint bash parquetmr:dev 

