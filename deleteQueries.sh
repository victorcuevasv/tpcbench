#!/bin/bash 

array=(19 27 3 34 42 43 46 52 53 55 59 63 65 68 7 73 79 8 82 89 98)

for i in "${array[@]}"
do
	mv query$i.sql query$i.txt
done

rm *.sql

for i in "${array[@]}"
do
	mv query$i.txt query$i.sql
done

