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
    echo "${yel}Usage: bash runclient_emrspark_createwithstep_qbeast.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

#Cluster configuration.
Nodes="2"
Version="6.2.0"
VersionShort="620"
AutoTerminate="true"
#Run configuration.
Tag="$(date +%s)"
ExperimentName="sparkemr-${VersionShort}-${Nodes}nodes-$1gb-$Tag"
DirNameWarehouse="tpcds-warehouse-sparkemr-${VersionShort}-$1gb-$2-$Tag"
DirNameResults="sparkemr-test"
DatabaseName="tpcds_sparkemr_${VersionShort}_$1gb_$2_$Tag"
JarFile="/mnt/benchmarking-jars-1664735376/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
JobName="TPCDS-test ${Tag}"
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
args[6]="--ext-raw-data-location=s3://benchmarking-datasets-1664540209/tpcds-data-1gb-csv-1664540949"
# prefix of external location for created tables (e.g. S3 bucket), null for none
args[7]="--ext-tables-location=s3://benchmarking-warehouses-1665074575/$DirNameWarehouse"
# format for column-storage tables (PARQUET, DELTA)
args[8]="--table-format=parquet"
# whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=false"

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
#number of runs to perform for the power test (default 1)
args[16]="--power-test-runs=1"
# flags (111111 schema|load|analyze|zorder|power|tput)
args[17]="--execution-flags=111010"

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
   "KeyName":"devtest_qbeast_nvirginia_keypair",
   "InstanceProfile":"EMR_EC2_DefaultRole",
   "SubnetId":"subnet-02b07eb9167614b28",
   "EmrManagedSlaveSecurityGroup":"sg-06ccc750d19f75561",
   "EmrManagedMasterSecurityGroup":"sg-00279d775135821e3",
   "AdditionalMasterSecurityGroups":["sg-014e2eff525ef62c5"]
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
         "org.bsc.dcc.vcv.RunBenchmarkSparkCLI",
         "$JarFile",
         $paramsStr
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


steps_func_hudi()
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
         "--conf",
         "spark.serializer=org.apache.spark.serializer.KryoSerializer",
         "--conf",
         "spark.sql.hive.convertMetastoreParquet=false",
		 "--jars",
		 "/usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar",
         "--class",
         "org.bsc.dcc.vcv.RunBenchmarkSparkCLI",
         "$JarFile",
         $paramsStr
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
      "InstanceCount":$Nodes,
      "InstanceGroupType":"CORE",
      "InstanceType":"m5.xlarge",
      "Name":"Core - $Nodes"
   },
   {
      "InstanceCount":1,
      "InstanceGroupType":"MASTER",
      "InstanceType":"m5.xlarge",
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
         "spark.driver.memory":"5692M",
         "hive.exec.max.dynamic.partitions":"3000",
         "hive.exec.dynamic.partition.mode":"nonstrict",
         "spark.sql.broadcastTimeout":"7200",
         "spark.sql.crossJoin.enabled":"true"
      }
   },
   { 
      "Classification": "spark-hive-site",
      "Properties": {
         "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
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
      "Path":"s3://emr-bootstrap-1665075461/s3fs/emr_init_qbeast.sh",
      "Args":[
         "hadoop",
         "benchmarking-jars-1664735376,benchmarking-results-1664735376"
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
steps=$(jq -c . <<<  "$(steps_func)")
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
	--applications Name=Hadoop Name=Hive Name=Spark \
	--ec2-attributes "$ec2Attributes" \
	--release-label emr-${Version} \
	--log-uri 's3n://emr-logs-1665075605/' \
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
	--region us-east-1 \
	--tags 'for-use-with-amazon-emr-managed-policies=true')

	cluster_id=$(jq -j '.ClusterId'  <<<  "$jsonCluster")
	echo "${blu}Created cluster with id ${cluster_id}.${end}"
fi

if [ "$WAIT_FOR_TERMINATION" -eq 1 ]; then
	echo "${blu}Waiting for the completion of cluster ${cluster_id}.${end}"
	wait_for_run_termination $cluster_id 120
	echo "${blu}Execution complete.${end}"
	if [ "$RUN_DELETE_WAREHOUSE" -eq 1 ]; then
    	echo "${blu}Deleting the warehouse directory ${DirNameWarehouse}.${end}"
    	aws s3 rm --recursive s3://benchmarking-warehouses-1665074575/$DirNameWarehouse
	fi
fi


