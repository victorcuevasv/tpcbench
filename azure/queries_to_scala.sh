#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

printf "\n\n%s\n\n" "${mag}Generating scala files.${end}"

q1to10file="q1to10.scala"
rm $q1to10file
printf "" > $q1to10file
printf "//START 3TB queries 1 to 10" >> $q1to10file
printf "\n\n" >> $q1to10file
for i in {1..10};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q1to10file
	cat query${i}.sql >> $q1to10file
	printf "\"\"\")" >> $q1to10file
	printf "\n\n" >> $q1to10file;
done
printf "//END 3TB queries 1 to 10" >> $q1to10file


q11to20file="q11to20.scala"
rm $q11to20file
printf "" > $q11to20file
printf "//START 3TB queries 11 to 20" >> $q11to20file
printf "\n\n" >> $q11to20file
for i in {11..20};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q11to20file
	cat query${i}.sql >> $q11to20file
	printf "\"\"\")" >> $q11to20file
	printf "\n\n" >> $q11to20file;
done
printf "//END 3TB queries 11 to 20" >> $q11to20file

q21to30file="q21to30.scala"
rm $q21to30file
printf "" > $q21to30file
printf "//START 3TB queries 21 to 30" >> $q21to30file
printf "\n\n" >> $q21to30file
for i in {21..30};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q21to30file
	cat query${i}.sql >> $q21to30file
	printf "\"\"\")" >> $q21to30file
	printf "\n\n" >> $q21to30file;
done
printf "//END 3TB queries 21 to 30" >> $q21to30file

q31to40file="q31to40.scala"
rm $q31to40file
printf "" > $q31to40file
printf "//START 3TB queries 31 to 40" >> $q31to40file
printf "\n\n" >> $q31to40file
for i in {31..40};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q31to40file
	cat query${i}.sql >> $q31to40file
	printf "\"\"\")" >> $q31to40file
	printf "\n\n" >> $q31to40file;
done
printf "//END 3TB queries 31 to 40" >> $q31to40file

q41to50file="q41to50.scala"
rm $q41to50file
printf "" > $q41to50file
printf "//START 3TB queries 41 to 50" >> $q41to50file
printf "\n\n" >> $q41to50file
for i in {41..50};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q41to50file
	cat query${i}.sql >> $q41to50file
	printf "\"\"\")" >> $q41to50file
	printf "\n\n" >> $q41to50file;
done
printf "//END 3TB queries 41 to 50" >> $q41to50file

q51to60file="q51to60.scala"
rm $q51to60file
printf "" > $q51to60file
printf "//START 3TB queries 51 to 60" >> $q51to60file
printf "\n\n" >> $q51to60file
for i in {51..60};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q51to60file
	cat query${i}.sql >> $q51to60file
	printf "\"\"\")" >> $q51to60file
	printf "\n\n" >> $q51to60file;
done
printf "//END 3TB queries 51 to 60" >> $q51to60file

q61to70file="q61to70.scala"
rm $q61to70file
printf "" > $q61to70file
printf "//START 3TB queries 61 to 70" >> $q61to70file
printf "\n\n" >> $q61to70file
for i in {61..70};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q61to70file
	cat query${i}.sql >> $q61to70file
	printf "\"\"\")" >> $q61to70file
	printf "\n\n" >> $q61to70file;
done
printf "//END 3TB queries 61 to 70" >> $q61to70file

q71to80file="q71to80.scala"
rm $q71to80file
printf "" > $q71to80file
printf "//START 3TB queries 71 to 80" >> $q71to80file
printf "\n\n" >> $q71to80file
for i in {71..80};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q71to80file
	cat query${i}.sql >> $q71to80file
	printf "\"\"\")" >> $q71to80file
	printf "\n\n" >> $q71to80file;
done
printf "//END 3TB queries 71 to 80" >> $q71to80file

q81to90file="q81to90.scala"
rm $q81to90file
printf "" > $q81to90file
printf "//START 3TB queries 81 to 90" >> $q81to90file
printf "\n\n" >> $q81to90file
for i in {81..90};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q81to90file
	cat query${i}.sql >> $q81to90file
	printf "\"\"\")" >> $q81to90file
	printf "\n\n" >> $q81to90file;
done
printf "//END 3TB queries 81 to 90" >> $q81to90file

q91to99file="q91to99.scala"
rm $q91to99file
printf "" > $q91to99file
printf "//START 3TB queries 91 to 99" >> $q91to99file
printf "\n\n" >> $q91to99file
for i in {91..99};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"" >> $q91to99file
	cat query${i}.sql >> $q91to99file
	printf "\"\"\")" >> $q91to99file
	printf "\n\n" >> $q91to99file;
done
printf "//END 3TB queries 91 to 99" >> $q91to99file




