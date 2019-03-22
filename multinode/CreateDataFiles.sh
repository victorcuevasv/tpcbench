#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Execute the createDataFiles.sh script of the parent directory.
#The .dat files generated will be inside a folder of the parent directory.

#Get the time of start of execution to measure total execution time.
start_time=`date +%s`

#Pass the scale factor first parameter to the script in the parent directory,
#along with the user and group id.s
bash ../createDataFiles.sh $1

end_time=`date +%s`

runtime=$((end_time-start_time))
printf "\n\n%s\n\n" "${cyn}Total execution time: ${runtime} sec.${end}"


