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
    echo "${yel}Usage: bash runclient_fullbenchmarkspark_awscli.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

BucketName="tpcds-warehouse-sparkemr-529-$1gb-$2"
DatabaseName="tpcds_emr529_$1gb_$2_db"

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
    exit 0
fi

RUN_DELETE_BUCKET=0

if [ "$RUN_DELETE_BUCKET" -eq 1 ]; then
    #Delete the bucket
    aws s3 rb s3://$BucketName --force
    exit 0
fi

exit 0 

args=()

#args[0] main work directory
args[0]="/mnt/efs/scratch/hadoop/data"
#args[1] schema (database) name
args[1]="$DatabaseName"
#args[2] results folder name (e.g. for Google Drive)
args[2]="19aoujv0ull8kx87l4by700xikfythorv"
#args[3] experiment name (name of subfolder within the results folder)
args[3]="sparkemr-529-2nodes-$1gb-deltapartwithzorder-test"
#args[4] system name (system name used within the logs)
args[4]="sparkemr"

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
args[10]="s3://tpcds-warehouse-sparkemr-529-$1gb-$2"
#args[11] format for column-storage tables (PARQUET, DELTA)
args[11]="parquet"
#args[12] whether to run queries to count the tuples generated (true/false)
args[12]="false"
#args[13] whether to use data partitioning for the tables (true/false)
args[13]="false"
#args[14] jar file
args[14]="/mnt/efs/FileStore/job-jars/project/targetsparkdatabricks/client-1.1-SNAPSHOT-jar-with-dependencies.jar"

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
#args[27] flags (111111 schema|load|analyze|zorder|power|tput)
args[27]="111011"

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
         "spark-submit",
         "--deploy-mode",
         "client",
         "--conf",
         "spark.eventLog.enabled=true",
         "--class",
         "org.bsc.dcc.vcv.RunBenchmarkSpark",
         "${args[14]}",
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
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"TERMINATE_CLUSTER",
      "Jar":"command-runner.jar",
      "Properties":"",
      "Name":"Spark application"
   }
]
EOF
}

instance-groups_func()
{
  cat <<EOF
[
   {
      "InstanceCount":2,
      "InstanceGroupType":"CORE",
      "InstanceType":"i3.2xlarge",
      "Name":"Core - 2"
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
      "Classification":"spark-defaults",
      "Properties":{
         "spark.driver.memory":"5692M",
         "hive.exec.max.dynamic.partitions":"3000",
         "hive.exec.dynamic.partition.mode":"nonstrict",
         "spark.sql.broadcastTimeout":"7200"
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
--applications Name=Hadoop Name=Hive Name=Spark \
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














