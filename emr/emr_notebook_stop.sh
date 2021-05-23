#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash emr_notebook_stop.sh <notebook-execution-id>${end}"
    exit 0
fi

jsonNotebook=$(aws emr --region us-west-2 \
stop-notebook-execution --notebook-execution-id $1)

#notebook_exec_id=$(jq -j '.NotebookExecutionId'  <<<  "$jsonNotebook")
#echo "${blu}Running notebook with id ${notebook_exec_id}.${end}"
echo "${blu}Stopping notebook: ${jsonNotebook}.${end}"

	