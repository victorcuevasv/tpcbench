#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_emrspark_createwithstep_hudi.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

#Cluster configuration.
Nodes="16"
Version="5.32.0"
VersionShort="532"
AutoTerminate="true"
#Run configuration.
#Tag="$(date +%s)huditest"
Tag="1616030068huditest" 
ExperimentName="sparkemr-${VersionShort}-${Nodes}nodes-$1gb-$Tag"
DirNameWarehouse="tpcds-warehouse-sparkemr-${VersionShort}-$1gb-$2-$Tag"
DirNameResults="sparkemr-${VersionShort}-test"
DatabaseName="tpcds_sparkemr_${VersionShort}_$1gb_$2_$Tag"
JarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
JobName="BSC-test ${Tag}"
#Script operation flags.
RUN_CREATE_CLUSTER=1
WAIT_FOR_TERMINATION=0
RUN_DELETE_WAREHOUSE=0

args=()
# main work directory
args[0]="--main-work-dir=/data"
# schema (database) name
args[1]="--schema-name=$DatabaseName"
# results folder name (e.g. for Google Drive)
args[2]="--results-dir=$DirNameResults"
# experiment name (name of subfolder within the results folder)
args[3]="--experiment-name=$ExperimentName"
# system name (system name used within the logs)
args[4]="--system-name=sparkemr"

# experiment instance number
args[5]="--instance-number=$2"
# prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[6]="--ext-raw-data-location=s3://tpcds-datasets/$1GB"
# prefix of external location for created tables (e.g. S3 bucket), null for none
args[7]="--ext-tables-location=s3://tpcds-warehouses-test/$DirNameWarehouse"
# format for column-storage tables (PARQUET, DELTA)
args[8]="--table-format=parquet"
# whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=true"

# "all" or create table file
args[10]="--all-or-create-file=all"
# jar file
args[11]="--jar-file=$JarFile"
# whether to generate statistics by analyzing tables (true/false)
args[12]="--use-row-stats=true"
# if argument above is true, whether to compute statistics for columns (true/false)
args[13]="--use-column-stats=true"
# "all" or query file
args[14]="--all-or-query-file=all"

# number of streams
args[15]="--number-of-streams=$3"
# flags 111111100000111111100
# schema      |load          |load denorm |load skip      |insupd data    |
# delete data |load update   |analyze     |analyze denorm |analyze update |
# zorder      |zorder update |read test 1 |insupd test    |read test 2    |
# delete test |read test 3   |gdpr        |read test 4    |power          |
# tput
#args[16]="--execution-flags=111111100000111111100"
args[16]="--execution-flags=000000100000111111100"
# count-queries
args[17]="--count-queries=false"
# all or denorm table file
args[18]="--denorm-all-or-file=store_sales.sql"
# skip data to be inserted later
args[19]="--denorm-apply-skip=true"

# all or query file for denorm analyze and z-order
args[20]="--analyze-zorder-all-or-file=query2.sql"
# customer surrogate key for the gdpr test
args[21]="--gdpr-customer-sk=221580"
# target size for hudi files
args[22]="--hudi-file-max-size=134217728" #1 GB: 1073741824, 128 MB: 134217728
# use merge on read for writing hudi files (use copy on write otherwise)
args[23]="--hudi-merge-on-read=true"
# enable compaction by default for merge on read tables
args[24]="--hudi-mor-default-compaction=false"

# force compaction between tests for merge on read tables
args[25]="--hudi-mor-force-compaction=false"
# greater than threshold for the date-sk attribute (2452459 for last 10%, -1 to disable)
args[26]="--datesk-gt-threshold=2452459"
# use a filter attribute and value for denormalization
args[27]="--denorm-with-filter=false"
# column-storage format for the update tests (hudi, iceberg)
args[28]="--update-table-format=hudi"

printf "\n\n%s\n\n" "${mag}Running the TPC-DS benchmark.${end}"

function auto_terminate_func() {
	if [[ $AutoTerminate == "true" ]] ; then
		echo " --auto-terminate "
	else	
		echo ""
	fi
}

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
   "EmrManagedMasterSecurityGroup":"sg-0cefde07cc1a0a36e",
   "AdditionalMasterSecurityGroups":["sg-e9663ea1"]
}
EOF
}

#Options for action on failure: TERMINATE_CLUSTER, CANCEL_AND_WAIT, and CONTINUE
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
         "org.bsc.dcc.vcv.RunBenchmarkSparkUpdateCLI",
         "$JarFile",
         $paramsStr
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"CONTINUE",
      "Jar":"command-runner.jar",
      "Properties":"",
      "Name":"Spark application"
   }
]
EOF
}

#Options for action on failure: TERMINATE_CLUSTER, CANCEL_AND_WAIT, and CONTINUE
steps_func_hudi()
{
  cat <<EOF
[
   {
      "Args":[
         "spark-submit",
         "--deploy-mode",
         "client",
         "--packages",
         "org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1",
         "--conf",
         "spark.eventLog.enabled=true",
         "--conf",
         "spark.serializer=org.apache.spark.serializer.KryoSerializer",
         "--conf",
         "spark.sql.hive.convertMetastoreParquet=false",
         "--class",
         "org.bsc.dcc.vcv.RunBenchmarkSparkUpdateCLI",
         "$JarFile",
         $paramsStr
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"CONTINUE",
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

#Use glue
#{
#    "Classification": "spark-hive-site",
#    "Properties": {
#      "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
#    }
#}
#Use external MySQL
#{
#      "Classification":"hive-site",
#      "Properties":{
#         "javax.jdo.option.ConnectionURL": "jdbc:mysql://metastoremysql.crhrootttpzi.us-west-2.rds.amazonaws.com:3306/hive?createDatabaseIfNotExist=true",
#         "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
#         "javax.jdo.option.ConnectionUserName": "hive",
#         "javax.jdo.option.ConnectionPassword": "hive"
#      }
#}
configurations_func()
{
  cat <<EOF
[
   {
      "Classification":"spark-defaults",
      "Properties":{
         "spark.driver.memory":"18971M",
         "spark.sql.broadcastTimeout":"7200",
         "spark.sql.crossJoin.enabled":"true"
      }
   },
   {
      "Classification":"hive-site",
      "Properties":{
         "javax.jdo.option.ConnectionURL": "jdbc:mysql://metastoremysql.crhrootttpzi.us-west-2.rds.amazonaws.com:3306/hive?createDatabaseIfNotExist=true",
         "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
         "javax.jdo.option.ConnectionUserName": "hive",
         "javax.jdo.option.ConnectionPassword": "hive"
      }
   }
]
EOF
}

#The emr_init.sh file in s3 corresponds to tpcdsbench/emr/bootstrap/s3fs/emr_init.sh
bootstrap-actions_func()
{
  cat <<EOF
[
   {
      "Path":"s3://bsc-bootstrap/s3fs/emr_init.sh",
      "Args":[
         "hadoop",
         "tpcds-jars,tpcds-results-test"
      ],
      "Name":"Custom action"
   }
]
EOF
}

#Get the state (RUNNING, TERMINATED) of a job run via its run_id using the Jobs REST API.
#$1 run_id
get_run_state() {
   declare jsonStr=$(aws emr describe-cluster --cluster-id $1)
   declare state=$(jq -j '.Cluster.Status.State'  <<<  "$jsonStr")
   echo $state
}

#Wait until the state of a job run is TERMINATED by polling using the Jobs REST API.
#$1 run_id
#$2 pause in seconds for polling
wait_for_run_termination() {
   declare state=$(get_run_state $1)
   while [ $state != "TERMINATED"  ] ;
   do
      sleep $2
      state=$(get_run_state $1)
   done
}

ec2Attributes=$(jq -c . <<<  "$(ec2-attributes_func)")
steps=$(jq -c . <<<  "$(steps_func_hudi)")
instanceGroups=$(jq -c . <<<  "$(instance-groups_func)")
configurations=$(jq -c . <<<  "$(configurations_func)")
bootstrapActions=$(jq -c . <<<  "$(bootstrap-actions_func)")

#Create the cluster and run the benchmark.

jsonCluster=""
cluster_id=""

#Example AWS CLI output.
#{
#	"ClusterId": "j-I7ZKG7UYDEDA",
#	"ClusterArn": "arn:aws:elasticmapreduce:us-west-2:384416317380:cluster/j-I7ZKG7UYDEDA"
#}
if [ "$RUN_CREATE_CLUSTER" -eq 1 ]; then
    jsonCluster=$(aws emr create-cluster \
	--termination-protected \
	--applications Name=Hadoop Name=Hive Name=Spark Name=Ganglia \
	--ec2-attributes "$ec2Attributes" \
	--release-label emr-${Version} \
	--log-uri 's3n://bsc-emr-logs/' \
	--steps "$steps" \
	--instance-groups "$instanceGroups" \
	--configurations "$configurations" $(auto_terminate_func) \
	--auto-scaling-role EMR_AutoScaling_DefaultRole \
	--bootstrap-actions "$bootstrapActions" \
	--ebs-root-volume-size 10 \
	--service-role EMR_DefaultRole \
	--enable-debugging \
	--name "$JobName" \
	--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
	--region us-west-2)

	cluster_id=$(jq -j '.ClusterId'  <<<  "$jsonCluster")
	echo "${blu}Created cluster with id ${cluster_id}.${end}"
fi

if [ "$WAIT_FOR_TERMINATION" -eq 1 ]; then
	echo "${blu}Waiting for the completion of cluster ${cluster_id}.${end}"
	wait_for_run_termination $cluster_id 120
	echo "${blu}Execution complete.${end}"
	if [ "$RUN_DELETE_WAREHOUSE" -eq 1 ]; then
    	echo "${blu}Deleting the warehouse directory ${DirNameWarehouse}.${end}"
    	aws s3 rm --recursive s3://tpcds-warehouses-test/$DirNameWarehouse
	fi
fi


