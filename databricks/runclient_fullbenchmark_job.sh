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

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmark_job.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

BucketName="tpcds-warehouse-$1gb-$2-deltanopartwithzorder-conc"
MountName="tpcds-warehouse-$1gb-$2-deltanopartwithzorder-conc-mnt"
DatabaseName="tpcds_warehouse_$1gb_$2_deltanopartwithzorder_conc_db"

data_mount_createdb_func()
{
  cat <<EOF
{
  "BucketName": "$BucketName",
  "MountName": "$MountName",
  "DatabaseName": "$DatabaseName"
}
EOF
}

RUN_MOUNT_CREATEDB_JOB=0

if [ "$RUN_MOUNT_CREATEDB_JOB" -eq 1 ]; then
    aws s3api create-bucket --bucket $BucketName --region us-west-2 --create-bucket-configuration LocationConstraint=us-west-2
    aws s3api put-bucket-tagging --bucket $BucketName --tagging 'TagSet=[{Key=Owner,Value=eng-benchmarking@databricks.com}]'
    aws s3api put-object --bucket $BucketName --key empty
    databricks jobs run-now --job-id 119 --notebook-params "$(data_mount_createdb_func)"
    exit 0
fi

exit 0

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating the job.${end}"

JOB_NAME="Run TPC-DS Benchmark"

args=()

#args[0] main work directory
args[0]="/data"
#args[1] schema (database) name
args[1]="$DatabaseName"
#args[2] results folder name (e.g. for Google Drive)
args[2]="19aoujv0ull8kx87l4by700xikfythorv"
#args[3] experiment name (name of subfolder within the results folder)
args[3]="dbr63-4nodes-$1gb-deltanopartwithzorder-createdb"
#args[4] system name (system name used within the logs)
args[4]="sparkdatabricks"

#args[5] experiment instance number
args[5]="$2"
#args[6] directory for generated data raw files
args[6]="UNUSED"
#args[7] subdirectory within the jar that contains the create table files
args[7]="tables"
#args[8] suffix used for intermediate table text files
args[8]="_ext"
#args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[9]="dbfs:/mnt/tpcdsbucket/$1GB"

#args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
args[10]="dbfs:/mnt/$MountName"
#args[11] format for column-storage tables (PARQUET, DELTA)
args[11]="delta"
#args[12] whether to run queries to count the tuples generated (true/false)
args[12]="false"
#args[13] whether to use data partitioning for the tables (true/false)
args[13]="false"
#args[14] jar file
args[14]="/dbfs/FileStore/job-jars/project/target/client-1.1-SNAPSHOT.jar"
 
#args[15] whether to generate statistics by analyzing tables (true/false)
args[15]="true"
#args[16] if argument above is true, whether to compute statistics for columns (true/false)
args[16]="true"
#args[17] queries dir within the jar
args[17]="QueriesSpark"
#args[18] subdirectory of work directory to store the results
args[18]="results"
#args[19] subdirectory of work directory to store the execution plans
args[19]="plans"

#args[20] save power test plans (boolean)
args[20]="true"
#args[21] save power test results (boolean)
args[21]="true"
#args[22] "all" or query file
args[22]="all"
#args[23] save tput test plans (boolean)
args[23]="true"
#args[24] save tput test results (boolean)
args[24]="true"
 
#args[25] number of streams
args[25]="$3"
#args[26] random seed
args[26]="1954"
#args[27] flags (11111 load|analyze|zorder|power|tput)
args[27]="11111"

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
            "spark.databricks.delta.autoCompact.maxFileSize":"134217728",
            "hive.exec.dynamic.partition.mode":"nonstrict",
            "hive.exec.max.dynamic.partitions":"3000"
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
         "num_workers":4
      },
      "libraries":[ 
         { 
            "jar":"dbfs:/FileStore/job-jars/project/target/client-1.1-SNAPSHOT.jar"
         }
      ],
      "email_notifications":{ 

      },
      "timeout_seconds":0,
      "spark_jar_task":{ 
         "jar_uri":"",
         "main_class_name":"org.bsc.dcc.vcv.RunBenchmarkLimit",
         "parameters":[ 
            "${args[0]}",
            "${args[1]}",
            "${args[2]}",
            "${args[3]}",
            "${args[4]}",
            
            "${args[5]}",
            "${args[6]}",
            "${args[7]}",
            "${args[8]}",
            "${args[9]}",
            
            "${args[10]}",
            "${args[11]}",
            "${args[12]}",
            "${args[13]}",
            "${args[14]}",
            
            "${args[15]}",
            "${args[16]}",
            "${args[17]}",
            "${args[18]}",
            "${args[19]}",
            
            "${args[20]}",
            "${args[21]}",
            "${args[22]}",
            "${args[23]}",
            "${args[24]}",
            
            "${args[25]}",
            "${args[26]}",
            "${args[27]}"
         ]
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




