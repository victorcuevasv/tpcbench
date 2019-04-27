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
    echo "${yel}Usage: bash create_S3_directories.sh <table names file> <S3 bucket name>${end}"
    echo "${yel}Example: bash create_S3_directories.sh tableNames.txt tpcds-warehouse-emr-presto-1gb${end}"
    exit 0
fi

#Read the file with the table names

#IFS=Internal Field Separator, used by the shell to determine how to do word splitting.
#Set it to empty to avoid leading/trailing whitespace trimming.
#For read, use the -r option so that backslash does not act as an escape character.
#The second conditional prevents the last line from being ignored if it doesn't end with a newline character.
#The -n option guarantees that the string has a length of more than zero.
IFS=''
while read -r line || [[ -n "$line" ]]; do
	IFS=' '
	for word in $line; do
		aws s3api put-object --bucket $2 --key $word/ 
	done
	IFS=''
done < "$1"



