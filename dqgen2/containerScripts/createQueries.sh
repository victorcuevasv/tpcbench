#!/bin/bash

#PARAMETERS.
#$1 scale

#Create a directory for the queries, if it doesn't exist
if [ ! -d ../output/QueriesNetezza ]; then
  mkdir ../output/QueriesNetezza
fi

#Create a temporary directory holding the query templates, copy to it the
#original templates and override those that needed to be changed.

printf "\n\n%s\n\n" "${blu}Generating queries.${end}"

for f in ../query_templates/query*.tpl ; do 
   ./dsqgen -template $(basename "$f") -OUTPUT_DIR ../output/QueriesNetezza -directory ../query_templates -dialect netezza -scale $1
   mv ../output/QueriesNetezza/query_0.sql ../output/QueriesNetezza/$(basename "$f" .tpl).sql  ; #.tpl is removed with this invocation of basename
done

