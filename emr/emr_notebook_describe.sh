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

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash emr_notebook_stop.sh <notebook-execution-id>${end}"
    exit 0
fi

jsonNotebook=$(aws emr --region us-west-2 \
describe-notebook-execution --notebook-execution-id $1)

echo "${blu}Describing notebook: ${jsonNotebook}.${end}"
notebook_status=$(jq -j '.NotebookExecution.Status'  <<<  "$jsonNotebook")
echo "${cyn}Notebook status ${notebook_exec_id}.${end}"
	