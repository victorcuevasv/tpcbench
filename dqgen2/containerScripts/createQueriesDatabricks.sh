#!/bin/bash
set -euxo pipefail
red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#PARAMETERS.
#$1 scale

#Create a directory for the Databricks queries, if it doesn't exist
if [ ! -d ../output/QueriesDatabricks ]; then
  mkdir ../output/QueriesDatabricks
fi

#Create a temporary directory holding the query templates, copy to it the
#original templates and override those that needed to be changed for Databricks.

printf "\n\n%s\n\n" "${blu}Preparing temporary query templates directory.${end}"

#Create the temporary directory for templates, if it doesn't exist.
#Clear its contents, otherwise.
if [ ! -d ../query_templates_temp ]; then
  mkdir ../query_templates_temp
else
  rm -f ../query_templates_temp/*
fi

#Copy the original templates.
cp ../query_templates/* ../query_templates_temp

#Override the modified templates for Databricks
cp ../query_templates_databricks/* ../query_templates_temp

printf "\n\n%s\n\n" "${blu}Generating Databricks queries.${end}"

#Process the templates in the created temporary directory.
#and output to the QueriesDatabricks directory.

for f in ../query_templates_temp/query*.tpl ; do 
   ./dsqgen -template $(basename "$f") -OUTPUT_DIR ../output/QueriesDatabricks -directory ../query_templates_temp -dialect databricks -scale $1
   mv ../output/QueriesDatabricks/query_0.sql ../output/QueriesDatabricks/$(basename "$f" .tpl).sql  ; #.tpl is removed with this invocation of basename
done

