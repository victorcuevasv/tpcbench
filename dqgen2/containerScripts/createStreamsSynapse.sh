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

#Create a directory for the Synapse queries, if it doesn't exist
if [ ! -d ../output/StreamsSynapse ]; then
  mkdir ../output/StreamsSynapse
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

#Override the modified templates for Synapse
cp ../query_templates_synapse/* ../query_templates_temp

#Override the ansi.tpl template with ansiLong.tpl
cp ../ansiLong.tpl ../query_templates_temp/ansi.tpl

printf "\n\n%s\n\n" "${blu}Generating the Synapse query streams with dsqgen.${end}"

./dsqgen -DIRECTORY ../query_templates_temp -INPUT ../query_templates_temp/templates.lst -OUTPUT_DIR ../output/StreamsSynapse -dialect ansi -scale $1 -streams $2   



  

