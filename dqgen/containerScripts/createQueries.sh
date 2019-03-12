#!/bin/bash

#Create a directory for the Presto queries, if it doesn't exist
if [ ! -d ../output/QueriesNetezza ]; then
  mkdir ../output/QueriesNetezza
fi

#Create a temporary directory holding the query templates, copy to it the
#original templates and override those that needed to be changed for presto.

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

for f in ../query_templates/query*.tpl ; do 
   ./dsqgen -template $(basename "$f") -OUTPUT_DIR ../output/QueriesNetezza -directory ../query_templates -dialect netezza -scale 1
   mv ../output/QueriesNetezza/query_0.sql ../output/QueriesNetezza/$(basename "$f" .tpl).sql  ; #.tpl is removed with this invocation of basename
done

