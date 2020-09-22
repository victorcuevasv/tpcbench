#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Generate the command to run the task in parallel.

#Set default values for the data generation parameters.
DEG_PAR=2
SCALE=1
TPCDS_VERSION=v2.13.0rc1
OUTDIR=/TPC-DS/$TPCDS_VERSION/output
DELIM=$(echo -e "\001")

#Obtain the parameters, if they are supplied, otherwise use the defaults.
if [ $# -eq 4 ]; then
   DEG_PAR=$1
   SCALE=$2
   OUTDIR=$3
   DELIM=$4
fi

cmd=""
pids=()

#Fork the processes and save their process ids to force a wait later.
for i in $(seq 1 $DEG_PAR);
do
   ./dsdgen -scale $SCALE -dir $OUTDIR -terminate n -delimiter $DELIM \
   -parallel $DEG_PAR -child $i &
   pids[${i}]=$!
done

printf "${mag}Running the command:${end}"
printf "\n\n${mag}$cmd${end}\n\n"

eval $cmd

#Wait for all pids to complete.
for pid in ${pids[*]}; do
    wait $pid
done

printf "${mag}Execution complete.${end}"



