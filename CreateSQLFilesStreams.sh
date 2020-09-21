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

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#PARAMETERS.
#$1 scale factor (positive integer)
#$2 number of streams (positive integer)

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash CreateSQLFiles.sh <scale factor> <number of streams>${end}"
    exit 0
fi

#Create SQL create table statement files and the query files.

#The runclient_processcreatescript script uses the java ProcessCreateScript class.
#Compile the java classes for the client first.
bash $DIR/compileCreateScript.sh

#First separate the multiple create table statements in the tpcds.sql file into separate files.
#Copy the file with create table statements into the datavol directory.

#cp $DIR/dqgen/v2.10.1rc3/tools/tpcds.sql $DIR/vols/data/tpcds.sql
cp $DIR/dqgen/tpcdsvarchar.sql $DIR/vols/data/tpcdsvarchar.sql
printf "\n\n%s\n\n" "${mag}Processing the tpcds[varchar].sql file.${end}"
#bash $DIR/runclient_processcreatescript.sh $USER_ID $GROUP_ID tpcds.sql
bash $DIR/runclient_processcreatescript.sh $USER_ID $GROUP_ID tpcdsvarchar.sql
cp -r $DIR/vols/data/tables $DIR/client/project/src/main/resources/

#Generate the unused Netezza queries.
printf "\n\n%s\n\n" "${mag}Generating the Netezza queries.${end}"
bash $DIR/dqgen/generateQueries.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesNetezza $DIR/client/project/src/main/resources/

#Generate the Presto queries.
printf "\n\n%s\n\n" "${mag}Generating the Presto queries.${end}"
bash $DIR/dqgen/generateQueriesPresto.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesPresto $DIR/client/project/src/main/resources/

#Generate the Spark queries.
printf "\n\n%s\n\n" "${mag}Generating the Spark queries.${end}"
bash $DIR/dqgen/generateQueriesSpark.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSpark $DIR/client/project/src/main/resources/

#Generate the Snowflake queries.
printf "\n\n%s\n\n" "${mag}Generating the Snowflake queries.${end}"
bash $DIR/dqgen/generateQueriesSnowflake.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSnowflake $DIR/client/project/src/main/resources/

#Generate the unused Netezza query streams.
printf "\n\n%s\n\n" "${mag}Generating the Netezza query streams.${end}"
bash $DIR/createStreams.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Netezza
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	cp -r $DIR/vols/data/StreamsNetezzaProcessed/stream$i $DIR/client/project/src/main/resources/QueriesNetezzaStream$i
done

#Generate the Spark query streams.
printf "\n\n%s\n\n" "${mag}Generating the Spark query streams.${end}"
bash $DIR/createStreamsSpark.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Spark
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	cp -r $DIR/vols/data/StreamsSparkProcessed/stream$i $DIR/client/project/src/main/resources/QueriesSparkStream$i
done

#Generate the Presto query streams.
printf "\n\n%s\n\n" "${mag}Generating the Presto query streams.${end}"
bash $DIR/createStreamsPresto.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Presto
#Add session properties to specific queries.
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	#Add SET SESSION statements to selected queries.
	printf "%s\n%s\n" "SET SESSION join_distribution_type = 'PARTITIONED';" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query5.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query5.sql     
	printf "%s\n" "SET SESSION join_distribution_type = 'AUTOMATIC';" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query5.sql
	
	printf "%s\n%s\n" "SET SESSION join_reordering_strategy = 'ELIMINATE_CROSS_JOINS';" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query18.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query18.sql     
	printf "%s\n" "SET SESSION join_reordering_strategy = 'ELIMINATE_CROSS_JOINS';" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query18.sql
	
	#printf "%s\n%s\n" "SET SESSION spill_enabled = false;" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query23.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query23.sql     
	#printf "%s\n" "SET SESSION spill_enabled = true;" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query23.sql
	
	#printf "%s\n%s\n" "SET SESSION spill_enabled = false;" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query30.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query30.sql     
	#printf "%s\n" "SET SESSION spill_enabled = true;" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query30.sql
	
	printf "%s\n%s\n" "SET SESSION task_concurrency = 32;" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query67.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query67.sql
	#printf "%s\n%s\n" "SET SESSION spill_enabled = false;" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query67.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query67.sql     
	printf "%s\n" "SET SESSION task_concurrency = 16;" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query67.sql
	#printf "%s\n" "SET SESSION spill_enabled = true;" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query67.sql
	
	printf "%s\n%s\n" "SET SESSION join_distribution_type = 'PARTITIONED';" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query75.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query75.sql     
	printf "%s\n" "SET SESSION join_distribution_type = 'AUTOMATIC';" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query75.sql
	
	printf "%s\n%s\n" "SET SESSION join_distribution_type = 'PARTITIONED';" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql     
	printf "%s\n%s\n" "SET SESSION join_reordering_strategy = 'NONE';" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql 
	#printf "%s\n%s\n" "SET SESSION spill_enabled = false;" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql
	printf "%s\n" "SET SESSION join_distribution_type = 'AUTOMATIC';" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql
	printf "%s\n" "SET SESSION join_reordering_strategy = 'AUTOMATIC';" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql
	#printf "%s\n" "SET SESSION spill_enabled = true;" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query78.sql
	
	printf "%s\n%s\n" "SET SESSION join_distribution_type = 'PARTITIONED';" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query80.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query80.sql     
	printf "%s\n" "SET SESSION join_distribution_type = 'AUTOMATIC';" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query80.sql
	     
	printf "%s\n%s\n" "SET SESSION join_reordering_strategy = 'NONE';" "$(cat $DIR/vols/data/StreamsPrestoProcessed/stream$i/query85.sql )" > $DIR/vols/data/StreamsPrestoProcessed/stream$i/query85.sql 
	printf "%s\n" "SET SESSION join_reordering_strategy = 'AUTOMATIC';" >> $DIR/vols/data/StreamsPrestoProcessed/stream$i/query85.sql
  
done
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	cp -r $DIR/vols/data/StreamsPrestoProcessed/stream$i $DIR/client/project/src/main/resources/QueriesPrestoStream$i
done

#Generate the Snowflake query streams.
printf "\n\n%s\n\n" "${mag}Generating the Snowflake query streams.${end}"
bash $DIR/createStreamsSnowflake.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Snowflake
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	cp -r $DIR/vols/data/StreamsSnowflakeProcessed/stream$i $DIR/client/project/src/main/resources/QueriesSnowflakeStream$i
done



