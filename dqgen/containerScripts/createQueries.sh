#!/bin/bash

for f in ../query_templates/query*.tpl ; do 
   ./dsqgen -template $(basename "$f") -OUTPUT_DIR ../output/QueriesNetezza -directory ../query_templates -dialect netezza -scale 1
   mv ../output/QueriesNetezza/query_0.sql ../output/QueriesNetezza/$(basename "$f" .tpl).sql  ; #.tpl is removed with this invocation of basename
done

