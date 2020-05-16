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
#$4 optional - descriptive tag for the experiment

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_emrspark_createwithstep_extmetastore.sh <scale factor> <experiment instance number> <number of streams> <tag>${end}"
    exit 0
fi

Tag="experimental$(date +%s)"

dummy4=$4
if [ ${#dummy4} -ge 1 ] ; then
        Tag=$4
fi

Nodes="2"
ExperimentName="sparkemr-529-${Nodes}nodes-$1gb-$Tag"
DirNameWarehouse="tpcds-warehouse-sparkemr-529-$1gb-$2-$Tag"
DirNameResults="1odwczxc3jftmhmvahdl7tz32dyyw0pen"
DatabaseName="tpcds_sparkemr_529_$1gb_$2_db_$Tag"
JarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
AutoTerminate="true"

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

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

#Important: the external RDS metastore may not work in EMR 6.0 for unknown reasons.
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
      "Classification":"hive-site",
      "Properties":{
         "javax.jdo.option.ConnectionURL": "jdbc:mysql://metastoremysql.crhrootttpzi.us-west-2.rds.amazonaws.com:3306/hive?createDatabaseIfNotExist=true",
         "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
         "javax.jdo.option.ConnectionUserName": "hive",
         "javax.jdo.option.ConnectionPassword": "hive",
         "hive.exec.max.dynamic.partitions":"5000",
         "hive.exec.dynamic.partition.mode":"nonstrict",
         "hive.exec.max.dynamic.partitions.pernode":"2500"
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

ec2Attributes=$(jq -c . <<<  "$(ec2-attributes_func)")
steps=$(jq -c . <<<  "$(steps_func)")
instanceGroups=$(jq -c . <<<  "$(instance-groups_func)")
configurations=$(jq -c . <<<  "$(configurations_func)")
bootstrapActions=$(jq -c . <<<  "$(bootstrap-actions_func)")

#Create the cluster and run the benchmark.

RUN_CREATE_CLUSTER=1
jsonCluster=""
cluster_id=""

if [ "$RUN_CREATE_CLUSTER" -eq 1 ]; then
    jsonCluster=$(aws emr create-cluster \
	--termination-protected \
	--applications Name=Hadoop Name=Hive Name=Spark \
	--ec2-attributes "$ec2Attributes" \
	--release-label emr-5.29.0 \
	--log-uri 's3n://bsc-emr-logs/' \
	--steps "$steps" \
	--instance-groups "$instanceGroups" \
	--configurations "$configurations" $(auto_terminate_func) \
	--auto-scaling-role EMR_AutoScaling_DefaultRole \
	--bootstrap-actions "$bootstrapActions" \
	--ebs-root-volume-size 10 \
	--service-role EMR_DefaultRole \
	--enable-debugging \
	--name 'BSC-test' \
	--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
	--region us-west-2)
    #Example output.
	#{
    #	"ClusterId": "j-I7ZKG7UYDEDA",
    #	"ClusterArn": "arn:aws:elasticmapreduce:us-west-2:384416317380:cluster/j-I7ZKG7UYDEDA"
	#}
	cluster_id=$(jq -j '.ClusterId'  <<<  "$jsonCluster")
	echo "${blu}Created cluster with id ${cluster_id}.${end}"
fi

WAIT_FOR_TERMINATION=0

if [ "$WAIT_FOR_TERMINATION" -eq 1 ]; then
	echo "${blu}Waiting for the completion of cluster ${cluster_id}.${end}"
	wait_for_run_termination $cluster_id 120
	echo "${blu}Execution complete.${end}"
fi

#Delete the warehouse directory.

RUN_DELETE_WAREHOUSE=0

if [ "$RUN_DELETE_WAREHOUSE" -eq 1 ]; then
    echo "${blu}Deleting the warehouse directory ${DirNameWarehouse}.${end}"
    #Delete the bucket
    aws s3 rm --recursive s3://tpcds-warehouses-test/$DirNameWarehouse
    #exit 0
fi

#The metastore has to be created only once, if it is not created on a new RDS server,
#the bootstrap process of the EMR cluster will fail. Subsequently, the same database
#can be used for all experiments.

#Commands to create the metastore
#Run on an EC-2 virtual machine with mysql installed (sudo yum install mysql -y)
#username:admin, password:maria_db
#mysql -h metastore.crhrootttpzi.us-west-2.rds.amazonaws.com -P 3306 -u admin -p
#create database hive; grant all privileges on hive.* to 'hive'@'%' identified by 'hive'; flush privileges;

#Commands to create the metastore
#Run on an EC-2 virtual machine with mysql installed (sudo yum install mysql -y)
#username:admin, password:mysqlmysql
#mysql -h metastoremysql.crhrootttpzi.us-west-2.rds.amazonaws.com -P 3306 -u admin -p
#create database hive; grant all privileges on hive.* to 'hive'@'%' identified by 'hive'; flush privileges;

#username:admin, password:postgres
#Options to install the postgresql client
#sudo amazon-linux-extras install postgresql10s
#sudo yum install postgresql
#psql -h [HOSTNAME] -p [PORT] -U [USERNAME] -W -d [DATABASENAME]
#psql -h metastorepg.crhrootttpzi.us-west-2.rds.amazonaws.com -p 5432 -U admin
#Create the database, Postgres cannot create it if it doesnt exist based on a JDBC parameter like MySQL.
#create database hive;
#create user hive with encrypted password 'hive';
#grant all privileges on database hive to hive;

#Create metastore schema. This can be necessary even if there is a pre-existent schema
#due to version problems. Note that the -dbType should be postgres or mysql, as required.
#/usr/lib/hive/bin/schematool -dbType postgres -initSchema

#Restart hive in EMR 5.x
#sudo stop hive-server2
#sudo stop hive-hcatalog-server
#sudo start hive-server2
#sudo start hive-hcatalog-server

#Start thrift server
#sudo /usr/lib/spark/sbin/start-thriftserver.sh

#Directory holding the driver jars for EMR
#/usr/lib/hive/lib
#/usr/lib/hive/lib/mariadb-connector-java.jar -> /usr/share/java/mariadb-connector-java.jar



