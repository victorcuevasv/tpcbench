#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 file with the table names

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash remove_S3_empty.sh <table names file> <S3 bucket name>${end}"
    echo "${yel}Example: bash remove_S3_empty.sh tableNames.txt tpcds-warehouse-emr-presto-1gb${end}"
    exit 0
fi

echo "${yel}Deleting files.${end}"

for (( i=$1; i<=$2; i++))
do
		#aws s3api delete-object --bucket $2 --key $word/empty
		aws s3 rm s3://tpcds-warehouse-databricks-1000gb-1/store_sales/ss_sold_date_sk=$i --recursive --quiet
		#echo $i  
done

echo "${yel}Files deleted.${end}"

