#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

jsonNotebook=$(aws emr --region us-west-2 \
start-notebook-execution \
--editor-id e-EYUEUFEDO1VW655D6L6LNS6KN \
--relative-path TPCDSLoadTableJarTest.ipynb \
--notebook-execution-name my-execution-TPCDSLoadTableJarTest \
--execution-engine '{"Id" : "j-13XAZUVSV5EK5"}' \
--service-role EMR_Notebooks_DefaultRole)

notebook_exec_id=$(jq -j '.NotebookExecutionId'  <<<  "$jsonNotebook")
echo "${blu}Running notebook with id ${notebook_exec_id}.${end}"
	
