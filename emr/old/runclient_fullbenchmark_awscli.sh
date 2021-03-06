#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmark_awscli.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

BucketName="tpcds-warehouse-prestoemr-529-$1gb-$2"
DatabaseName="tpcds_prestoemr529_$1gb_$2_db"
Nodes="8"

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

RUN_CREATE_BUCKET=0

if [ "$RUN_CREATE_BUCKET" -eq 1 ]; then
    #Create the bucket
    aws s3api create-bucket --bucket $BucketName --region us-west-2 --create-bucket-configuration LocationConstraint=us-west-2
    #Add the Owner tag
    aws s3api put-bucket-tagging --bucket $BucketName --tagging 'TagSet=[{Key=Owner,Value=eng-benchmarking@databricks.com}]'
    #Block all public access for the bucket
    aws s3api put-public-access-block --bucket $BucketName --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"  
    #Create and empty folder to enable mounting
    aws s3api put-object --bucket $BucketName --key empty
    #Create the table folders
    tables=(call_center catalog_page catalog_returns catalog_sales customer customer_address \
			customer_demographics date_dim dbgen_version household_demographics income_band \
			inventory item promotion reason ship_mode store store_returns store_sales time_dim \
			warehouse web_page web_returns web_sales web_site)
	for t in "${tables[@]}"
	do
		#Must specify through the content type that it is a directory and use the trailing slash.
		aws s3api put-object --bucket $BucketName --content-type application/x-directory --key $t/
	done
    exit 0
fi

RUN_DELETE_BUCKET=0

if [ "$RUN_DELETE_BUCKET" -eq 1 ]; then
    #Delete the bucket
    aws s3 rb s3://$BucketName --force
    exit 0
fi

args=()

#args[0] main work directory
args[0]="/mnt/efs/scratch/hadoop/data"
#args[1] schema (database) name
args[1]="$DatabaseName"
#args[2] results folder name (e.g. for Google Drive)
args[2]="19aoujv0ull8kx87l4by700xikfythorv"
#args[3] experiment name (name of subfolder within the results folder)
args[3]="prestoemr-529-${Nodes}nodes-$1gb"
#args[4] system name (system name used within the logs)
args[4]="prestoemr"

#args[5] experiment instance number
args[5]="$2"
#args[6] directory for generated data raw files
args[6]="UNUSED"
#args[7] subdirectory within the jar that contains the create table files
args[7]="tables"
#args[8] suffix used for intermediate table text files
args[8]="_ext"
#args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[9]="s3://tpcds-datasets/$1GB"

#args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
args[10]="s3://$BucketName"
#args[11] format for column-storage tables (PARQUET, DELTA)
args[11]="orc"
#args[12] whether to run queries to count the tuples generated (true/false)
args[12]="false"
#args[13] whether to use data partitioning for the tables (true/false)
args[13]="false"
#args[14] whether to use bucketing for Hive and Presto
args[14]="true"

#args[15] hostname of the server
args[15]="localhost"
#args[16] username for the connection
args[16]="hadoop"
#args[17] jar file
args[17]="/mnt/efs/FileStore/job-jars/project/targetemr/client-1.2-SNAPSHOT-jar-with-dependencies.jar"
#args[18] whether to generate statistics by analyzing tables (true/false)
args[18]="true"
#args[19] if argument above is true, whether to compute statistics for columns (true/false)
args[19]="true"

#args[20] queries dir within the jar
args[20]="QueriesPresto"
#args[21] subdirectory of work directory to store the results
args[21]="results"
#args[22] subdirectory of work directory to store the execution plans
args[22]="plans"
#args[23] save power test plans (boolean)
args[23]="true"
#args[24] save power test results (boolean)
args[24]="true"

#args[25] "all" or query file
args[25]="all" 
#args[26] save tput test plans (boolean)
args[26]="true"
#args[27] save tput test results (boolean)
args[27]="true"
#args[28] number of streams
args[28]="$3"
#args[29] random seed
args[29]="1954"

#args[30] use multiple connections (true|false)
args[30]="true"
#args[31] flags (111111 schema|load|analyze|zorder|power|tput)
args[31]="111011"

function json_string_list() {
    declare array=("$@")
    declare list=""
    for w in "${array[@]}"
    do
        list+="\"$w\", "
    done
    #Remove the last comma and space
    echo ${list%??}
}

paramsStr=$(json_string_list "${args[@]}")

ec2-attributes_func()
{
  cat <<EOF
{
   "KeyName":"testalojakeypair",
   "InstanceProfile":"EMR_EC2_DefaultRole",
   "SubnetId":"subnet-01033078",
   "EmrManagedSlaveSecurityGroup":"sg-0d6c7aa7f3a231e50",
   "EmrManagedMasterSecurityGroup":"sg-0cefde07cc1a0a36e"
}
EOF
}


steps_func()
{
  cat <<EOF
[
   {
      "Args":[
		$paramsStr
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"TERMINATE_CLUSTER",
      "Jar":"/mnt/efs/FileStore/job-jars/project/targetemr/client-1.2-SNAPSHOT-jar-with-dependencies.jar",
      "Properties":"",
      "Name":"Custom JAR"
   }
]
EOF
}

instance-groups_func()
{
  cat <<EOF
[
   {
      "InstanceCount":$Nodes,
      "InstanceGroupType":"CORE",
      "InstanceType":"i3.2xlarge",
      "Name":"Core - $Nodes"
   },
   {
      "InstanceCount":1,
      "InstanceGroupType":"MASTER",
      "InstanceType":"i3.2xlarge",
      "Name":"Master - 1"
   }
]
EOF
}


configurations_func()
{
  cat <<EOF
[
   {
      "Classification":"presto-connector-hive",
      "Properties":{
         "hive.allow-drop-table":"true",
         "hive.compression-codec":"SNAPPY",
         "hive.max-partitions-per-writers":"2500",
         "hive.s3-file-system-type":"PRESTO"
      }
   },
   {
      "Classification":"presto-config",
      "Properties":{
         "experimental.spiller-spill-path":"/mnt/tmp/",
         "experimental.max-spill-per-node":"1400GB",
         "experimental.query-max-spill-per-node":"700GB",
         "experimental.spill-enabled":"false",
         "experimental.spill-compression-enabled":"true",
         "query.max-memory":"240GB",
         "query.max-memory-per-node":"27GB",
         "query.max-total-memory-per-node":"29GB"
      }
   },
   {
      "Classification":"hive-site",
      "Properties":{
         "hive.exec.max.dynamic.partitions":"5000",
         "hive.exec.dynamic.partition.mode":"nonstrict",
         "hive.exec.max.dynamic.partitions.pernode":"2500"
      }
   }
]
EOF
}


bootstrap-actions_func()
{
  cat <<EOF
[
   {
      "Path":"s3://bsc-bootstrap/emrClusterEFS_user_param.sh",
      "Args":[
         "hadoop"
      ],
      "Name":"Custom action"
   }
]
EOF
}


ec2Attributes=$(jq -c . <<<  "$(ec2-attributes_func)")
steps=$(jq -c . <<<  "$(steps_func)")
instanceGroups=$(jq -c . <<<  "$(instance-groups_func)")
configurations=$(jq -c . <<<  "$(configurations_func)")
bootstrapActions=$(jq -c . <<<  "$(bootstrap-actions_func)")


aws emr create-cluster \
--termination-protected \
--applications Name=Hadoop Name=Hive Name=Presto Name=Ganglia \
--ec2-attributes "$ec2Attributes" \
--release-label emr-5.29.0 \
--log-uri 's3n://bsc-emr-logs/' \
--steps "$steps" \
--instance-groups "$instanceGroups" \
--configurations "$configurations" \
--auto-terminate \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--bootstrap-actions "$bootstrapActions" \
--ebs-root-volume-size 10 \
--service-role EMR_DefaultRole \
--enable-debugging \
--name 'BSC-test' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-west-2













