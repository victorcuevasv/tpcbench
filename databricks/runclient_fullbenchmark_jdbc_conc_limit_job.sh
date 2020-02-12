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

#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)
#$4 number of workers (positive integer)

if [ $# -lt 4 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmark_job.sh <scale factor> <experiment instance number> <number of streams> <number of workers>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating the job.${end}"

JOB_NAME="Connection test"

#args[0] main work directory
ARGS0="/data"
#args[1] schema (database) name
ARGS1="tpcdsdb$1gb_$2_deltanopartwithzorder"
#args[2] results folder name (e.g. for Google Drive)
ARGS2="1pi53t9ak8siwmdi22zrjzu5fk-1b9gvp"
#args[3] experiment name (name of subfolder within the results folder)
ARGS3="databricks61-8nodesconnections$3streams$4threads"
#args[4] system name (system name used within the logs)
ARGS4="sparkdatabricksjdbc"
 
#args[5] experiment instance number
ARGS5="$2"
#args[6] directory for generated data raw files
ARGS6="UNUSED"
#args[7] subdirectory within the jar that contains the create table files
ARGS7="tables"
#args[8] suffix used for intermediate table text files
ARGS8="_ext"
#args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
ARGS9="dbfs:/mnt/tpcdsbucket/$1GB"
 
#args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
ARGS10="dbfs:/mnt/tpcdswarehousebucket$1GB_$2"
#args[11] format for column-storage tables (PARQUET, DELTA)
ARGS11="parquet"
#args[12] whether to run queries to count the tuples generated (true/false)
ARGS12="false"
#args[13] whether to use data partitioning for the tables (true/false)
ARGS13="false"
#args[14] hostname of the server
ARGS14="dbc-08fc9045-faef.cloud.databricks.com"
 
#args[15] username for the connection
ARGS15="UNUSED"
#args[16] jar file
ARGS16="/project/targetsparkjdbc/client-1.0-SNAPSHOT.jar"
#args[17] whether to generate statistics by analyzing tables (true/false)
ARGS17="false"
#args[18] if argument above is true, whether to compute statistics for columns (true/false)
ARGS18="UNUSED"
#args[19] queries dir within the jar
ARGS19="QueriesSpark"

#args[20] subdirectory of work directory to store the results
ARGS20="results"
#args[21] subdirectory of work directory to store the execution plans
ARGS21="plans"
#args[22] save power test plans (boolean)
ARGS22="false"
#args[23] save power test results (boolean)
ARGS23="true"
#args[24] "all" or query file
ARGS24="all"

#args[25] save tput test plans (boolean)
ARGS25="false"
#args[26] save tput test results (boolean)
ARGS26="true"
#args[27] number of streams
ARGS27="$3"
#args[28] random seed
ARGS28="1954"
#args[29] number of workers
ARGS29="$4"

#args[30] use multiple connections (true|false)
ARGS30="true"

post_data_func()
{
  cat <<EOF
{ 
	"name":"$JOB_NAME",
	"new_cluster":{ 
		"spark_version":"6.3.x-scala2.11",
        "spark_conf":{
        	"spark.databricks.delta.optimize.maxFileSize":"134217728",
            "spark.databricks.delta.optimize.minFileSize":"134217728",
            "spark.sql.crossJoin.enabled":"true",
            "spark.databricks.optimizer.deltaTableFilesThreshold":"100",
            "spark.sql.broadcastTimeout":"7200",
            "spark.databricks.delta.autoCompact.maxFileSize":"134217728"
         },
         "aws_attributes":{ 
            "zone_id":"us-west-2b",
            "availability":"ON_DEMAND"
         },
         "node_type_id":"i3.2xlarge",
         "ssh_public_keys":[ 
            "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDjxM9WRCj8vI0aDAVhVi3DkibIaChqwjshgcPvXcth5l1cWxBZLkDdL1CCLSAbUGGL+HX79FL7L46wBCOHTJ2hhd7tjxxDG7IeH/G2Q1wPsTMt7Vpswc8Ijp6BKzbqAJS9HJAq9VPjh0x39gPd2x4vHiRpudKA+RFvTQ1jWRz0nTxI/eteZWB03jrPQxbZFo5v/29VDTwRlDBraC5q3hblfXAUk8cWQkhlFz2XagPNzsigVsY/zJvIJ/zW5ZpPI5El2VK3CEGjqbg5qt7QnIiRydly3N2eWHDIwZM3nfAMYdWig+65U8LOy9NC8J6dk8v/ZlstoOLNNm5+LSkmj9b7 pristine@al-1001"   
         ],
         "enable_elastic_disk":false,
         "num_workers":8
      },
      "libraries":[ 
         { 
            "jar":"dbfs:/FileStore/job-jars/project/targetsparkdatabricks/client-1.0-SNAPSHOT-jar-with-dependencies-noz.jar"
         }
      ],
      "email_notifications":{ 

      },
      "timeout_seconds":0,
      "spark_jar_task":{ 
         "jar_uri":"",
         "main_class_name":"org.bsc.dcc.vcv.RunBenchmarkLimit",
         "parameters":[ 
            "$ARGS0",
            "$ARGS1",
            "$ARGS2",
            "$ARGS3",
            "$ARGS4",
            
            "$ARGS5",
            "$ARGS6",
            "$ARGS7",
            "$ARGS8",
            "$ARGS9",
            
            "$ARGS10",
            "$ARGS11",
            "$ARGS12",
            "$ARGS13",
            "$ARGS14",
            
            "$ARGS15",
            "$ARGS16",
            "$ARGS17",
            "$ARGS18",
            "$ARGS19",
            
            "$ARGS20",
            "$ARGS21",
            "$ARGS22",
            "$ARGS23",
            "$ARGS24",
            
            "$ARGS25",
            "$ARGS26",
            "$ARGS27",
            "$ARGS28",
            "$ARGS29",
            
            "$ARGS30"
         ],
      },
      "max_concurrent_runs":1
}
EOF
}


curl -X POST \
-H "Authorization: Bearer $(echo $DATABRICKS_TOKEN)" \
-H "Content-Type: application/json" \
-d "$(post_data_func)" \
https://dbc-08fc9045-faef.cloud.databricks.com/api/2.0/jobs/create




