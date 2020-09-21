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
    echo "${yel}Usage: bash runclient_emrspark_createwithstep_multiple.sh <scale factor> <number of experiment instances> <number of streams>${end}"
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Running the TPC-DS benchmark.${end}"

printf "\n\n%s\n\n" "${mag}Creating the clusters.${end}"

#Cluster configuration.
Nodes="2"
Version="5.29.0"
VersionShort="529"
AutoTerminate="true"
#Run configuration.
Tag="$(date +%s)test"
JarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
#Script operation flags.
RUN_CREATE_CLUSTER=1

JobName=""
args=()
paramsStr=""

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
      "Classification":"spark-defaults",
      "Properties":{
         "spark.driver.memory":"5692M",
         "hive.exec.max.dynamic.partitions":"3000",
         "hive.exec.dynamic.partition.mode":"nonstrict",
         "spark.sql.broadcastTimeout":"7200",
         "spark.sql.crossJoin.enabled":"true"
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

START=1
END=$2
for (( i=$START; i<=$END; i++ ))
do
	ExperimentName="sparkemr-${VersionShort}-${Nodes}nodes-$1gb-$Tag"
	DirNameWarehouse="tpcds-warehouse-sparkemr-${VersionShort}-$1gb-${i}-$Tag"
	DirNameResults="sparkemr-test"
	DatabaseName="tpcds_sparkemr_${VersionShort}_$1gb_${i}_$Tag"
	JobName="Run TPC-DS Benchmark ${i}"

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
	args[5]="--instance-number=${i}"
	# prefix of external location for raw data tables (e.g. S3 bucket), null for none
	args[6]="--ext-raw-data-location=s3://tpcds-datasets/$1GB"
	# prefix of external location for created tables (e.g. S3 bucket), null for none
	args[7]="--ext-tables-location=s3://tpcds-warehouses-test/$DirNameWarehouse"
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
	args[14]="--all-or-query-file=query2.sql"
	 
	# number of streams
	args[15]="--number-of-streams=$3"
	# flags (111111 schema|load|analyze|zorder|power|tput)
	args[16]="--execution-flags=111011"

	paramsStr=$(json_string_list "${args[@]}")
	
	ec2Attributes=$(jq -c . <<<  "$(ec2-attributes_func)")
	steps=$(jq -c . <<<  "$(steps_func)")
	instanceGroups=$(jq -c . <<<  "$(instance-groups_func)")
	configurations=$(jq -c . <<<  "$(configurations_func)")
	bootstrapActions=$(jq -c . <<<  "$(bootstrap-actions_func)")
	
	jsonCluster=""
	cluster_id=""

	if [ "$RUN_CREATE_CLUSTER" -eq 1 ]; then
	    jsonCluster=$(aws emr create-cluster \
		--termination-protected \
		--applications Name=Hadoop Name=Hive Name=Spark \
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
	args=()
done

