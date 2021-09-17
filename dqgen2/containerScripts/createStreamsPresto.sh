#!/bin/bash
set -euxo pipefail
#PARAMETERS.
#$1 scale
#$2 number of streams

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Create a directory for the Presto queries, if it doesn't exist
if [ ! -d ../output/StreamsPresto ]; then
  mkdir ../output/StreamsPresto
fi

#Create a temporary directory holding the query templates, copy to it the
#original templates and override the netezza.tpl template with netezzaLong.tpl.

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

#Override the modified templates for Presto
cp ../query_templates_presto/* ../query_templates_temp

#Override the netezza.tpl template with netezzaLong.tpl
cp ../netezzaLong.tpl ../query_templates_temp/netezza.tpl

printf "\n\n%s\n\n" "${blu}Generating the Presto query streams with dsqgen.${end}"

./dsqgen -DIRECTORY ../query_templates_temp -INPUT ../query_templates_temp/templates.lst -OUTPUT_DIR ../output/StreamsPresto -dialect netezza -scale $1 -streams $2   


