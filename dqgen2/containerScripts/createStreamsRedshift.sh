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

#Create a directory for the Redshift queries, if it doesn't exist
if [ ! -d ../output/StreamsRedshift ]; then
  mkdir ../output/StreamsRedshift
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

#Override the modified templates for Redshift
cp ../query_templates_redshift/* ../query_templates_temp
for f in ../query_templates_temp/query*.tpl ; do
  sed -i 's/substr/substring/g' $f 
  sed -i 's/ days)/)/g' $f
done

#Override the netezza.tpl template with netezzaLong.tpl
cp ../netezzaLong.tpl ../query_templates_temp/netezza.tpl

printf "\n\n%s\n\n" "${blu}Generating the Redshift query streams with dsqgen.${end}"

./dsqgen -DIRECTORY ../query_templates_temp -INPUT ../query_templates_temp/templates.lst -OUTPUT_DIR ../output/StreamsRedshift -dialect netezza -scale $1 -streams $2   



  

