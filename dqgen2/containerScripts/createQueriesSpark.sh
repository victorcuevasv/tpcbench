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

#Create a directory for the Spark queries, if it doesn't exist
if [ ! -d ../output/QueriesSpark ]; then
  mkdir ../output/QueriesSpark
fi

#Create a temporary directory holding the query templates, copy to it the
#original templates and override those that needed to be changed for Spark.

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

#Override the modified templates for Spark
cp ../query_templates_spark/* ../query_templates_temp

printf "\n\n%s\n\n" "${blu}Generating Spark queries.${end}"

#Process the templates in the created temporary directory.
#and output to the QueriesSpark directory.

for f in ../query_templates_temp/query*.tpl ; do 
   ./dsqgen -template $(basename "$f") -OUTPUT_DIR ../output/QueriesSpark -directory ../query_templates_temp -dialect netezza -scale $1
   mv ../output/QueriesSpark/query_0.sql ../output/QueriesSpark/$(basename "$f" .tpl).sql  ; #.tpl is removed with this invocation of basename
done

