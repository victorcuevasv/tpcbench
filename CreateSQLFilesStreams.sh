#!/bin/bash
set -x
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

cp $DIR/dqgen2/tpcdsvarchar.sql $DIR/vols/data/tpcdsvarchar.sql
printf "\n\n%s\n\n" "${mag}Processing the tpcds[varchar].sql file.${end}"
bash $DIR/runclient_processcreatescript.sh $USER_ID $GROUP_ID tpcdsvarchar.sql tables
cp -r $DIR/vols/data/tables $DIR/client/project/src/main/resources/

cp $DIR/dqgen2/tpcdsstring.sql $DIR/vols/data/tpcdsstring.sql
printf "\n\n%s\n\n" "${mag}Processing the tpcds[string].sql file.${end}"
bash $DIR/runclient_processcreatescript.sh $USER_ID $GROUP_ID tpcdsstring.sql tablesstring
cp -r $DIR/vols/data/tablesstring $DIR/client/project/src/main/resources/

#Generate the Databricks queries.
printf "\n\n%s\n\n" "${mag}Generating the Databricks queries.${end}"
bash $DIR/dqgen2/generateQueriesDatabricks.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesDatabricks $DIR/client/project/src/main/resources/

# #Generate the Presto queries.
# printf "\n\n%s\n\n" "${mag}Generating the Presto queries.${end}"
# bash $DIR/dqgen2/generateQueriesPresto.sh $USER_ID $GROUP_ID $1
# cp -r $DIR/vols/data/QueriesPresto $DIR/client/project/src/main/resources/

#Generate the Spark queries.
printf "\n\n%s\n\n" "${mag}Generating the Spark queries.${end}"
bash $DIR/dqgen2/generateQueriesSpark.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSpark $DIR/client/project/src/main/resources/

#Generate the Snowflake queries.
printf "\n\n%s\n\n" "${mag}Generating the Snowflake queries.${end}"
bash $DIR/dqgen2/generateQueriesSnowflake.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSnowflake $DIR/client/project/src/main/resources/

#Generate the Redshift queries.
printf "\n\n%s\n\n" "${mag}Generating the Redshift queries.${end}"
bash $DIR/dqgen2/generateQueriesRedshift.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesRedshift $DIR/client/project/src/main/resources/

#Generate the Synapse queries.
printf "\n\n%s\n\n" "${mag}Generating the Synapse queries.${end}"
bash $DIR/dqgen2/generateQueriesSynapse.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSynapse $DIR/client/project/src/main/resources/

#Generate the BigQuery queries.
printf "\n\n%s\n\n" "${mag}Generating the BigQuery queries.${end}"
bash $DIR/dqgen2/generateQueriesBigQuery.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesBigQuery $DIR/client/project/src/main/resources/

#Generate the Databricks query streams.
printf "\n\n%s\n\n" "${mag}Generating the Databricks query streams.${end}"
bash $DIR/createStreams.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Databricks
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	mkdir $DIR/client/project/src/main/resources/QueriesDatabricksStream$i
	cp $DIR/vols/data/StreamsDatabricksProcessed/stream$i/* $DIR/client/project/src/main/resources/QueriesDatabricksStream$i
done

#Generate the Spark query streams.
printf "\n\n%s\n\n" "${mag}Generating the Spark query streams.${end}"
bash $DIR/createStreamsSpark.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Spark
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	mkdir $DIR/client/project/src/main/resources/QueriesSparkStream$i
	cp $DIR/vols/data/StreamsSparkProcessed/stream$i/* $DIR/client/project/src/main/resources/QueriesSparkStream$i
done

# #Generate the Presto query streams.
# printf "\n\n%s\n\n" "${mag}Generating the Presto query streams.${end}"
# bash $DIR/createStreamsPresto.sh $1 $2
# bash $DIR/runclient_processStreamFilesQueries.sh Presto
# START=0
# END=$2
# for (( i=$START; i<$END; i++ ))
# do
# 	mkdir $DIR/client/project/src/main/resources/QueriesPrestoStream$i
# 	cp $DIR/vols/data/StreamsPrestoProcessed/stream$i/* $DIR/client/project/src/main/resources/QueriesPrestoStream$i
# done

#Generate the Snowflake query streams.
printf "\n\n%s\n\n" "${mag}Generating the Snowflake query streams.${end}"
bash $DIR/createStreamsSnowflake.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Snowflake
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	mkdir $DIR/client/project/src/main/resources/QueriesSnowflakeStream$i
	cp $DIR/vols/data/StreamsSnowflakeProcessed/stream$i/* $DIR/client/project/src/main/resources/QueriesSnowflakeStream$i
done

#Generate the Redshift query streams.
printf "\n\n%s\n\n" "${mag}Generating the Redshift query streams.${end}"
bash $DIR/createStreamsRedshift.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Redshift
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	mkdir $DIR/client/project/src/main/resources/QueriesRedshiftStream$i
	cp $DIR/vols/data/StreamsRedshiftProcessed/stream$i/* $DIR/client/project/src/main/resources/QueriesRedshiftStream$i
done

#Generate the Synapse query streams.
printf "\n\n%s\n\n" "${mag}Generating the Synapse query streams.${end}"
bash $DIR/createStreamsSynapse.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh Synapse
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	mkdir $DIR/client/project/src/main/resources/QueriesSynapseStream$i
	cp $DIR/vols/data/StreamsSynapseProcessed/stream$i/* $DIR/client/project/src/main/resources/QueriesSynapseStream$i
done

#Generate the BigQuery query streams.
printf "\n\n%s\n\n" "${mag}Generating the BigQuery query streams.${end}"
bash $DIR/createStreamsBigQuery.sh $1 $2
bash $DIR/runclient_processStreamFilesQueries.sh BigQuery
START=0
END=$2
for (( i=$START; i<$END; i++ ))
do
	mkdir $DIR/client/project/src/main/resources/QueriesBigQueryStream$i
	cp $DIR/vols/data/StreamsBigQueryProcessed/stream$i/* $DIR/client/project/src/main/resources/QueriesBigQueryStream$i
done





