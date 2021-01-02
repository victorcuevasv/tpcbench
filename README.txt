
INSTALLATION INSTRUCTIONS

It is recommended to use a VM on AWS to run the experiments for ease of use, reproducibility of results, 
and to minimize network latency for some experiments. To create the AWS VM and install the benchmarking
platform follow the next steps.

1) Create a VM to run the experiments, the VM can be created directly from the AWS web GUI:

EC2 > Launch templates > select TPCDSBenchmarkVM template > Actions > Launch instance from template

Preferably change the Name of the VM in the <Resource tags> section.

The benchmark system can then be downloaded to the newly created VM in order to run benchmarking experiments.

git clone https://github.com/HiEST/tpcdsbench

2) Build the benchmarking infrastructure.

The building process will create the container images to generate the queries, compile the application,
and run experiments. Special steps are required for the Databricks JDBC application, due to the manner
the Databricks JDBC driver is distributed.

An http server should be setup first to make the Databricks JDBC driver accessible.


2.1) Copy the Databricks JDBC driver

First, create the directory that will contain the driver jar, run following command inside the tpcdsbench
directory:

mkdir -p tarserver/static-html-directory/simbadriver

The Databricks JDBC driver jar is stored in:

s3://static-html-directory/simbadriver/SparkJDBC42.jar

If permissions to copy it directly are not available, copy it manually into the directory created previously
via sftp.


2.2) Create the http server to make the Databricks JDBC driver available:

The http server relies on an Apache http server container image, create and start the container:

bash tarserver/build_server.sh

bash tarserver/run_server.sh


2.3) Create the containers.

The BuildAll.sh script creates the necessary container images. Inside the tpcdsbench directory execute this
script with

JDBC=true bash emr/BuildAll.sh

The container images created shoud include: clientbuilder, tpcds, ubuntujava, and ubuntu. The clientbuilder
image contains the benchmarking application code, while the tpcds image contains the data and queries
generation tools adapted from the TPC-DS Toolkit.


3) Compile the application

After the benchmarking infrastructure has been installed, in order to run experiments it is still necessary
to generate the SQL files (create table statements and queries) and to compile the benchmarking application.
It is important to remark that the benchmarking application and the SQL files it will run are encapsulated
in the same jar file. However, the generation of the queries and the compilation of the application are 
separate tasks.

3.1) Create the SQL files

It is absolutely necessary to create the SQL files before compiling the application, otherwise the SQL files
will not be included in the jar. Inside the tpcdsbench directory, to create the SQL files execute the following
script, supplying the scale factor (as an integer denoting gigabytes):

bash CreateSQLFilesStreams.sh <scale factor> <number of streams>

For example, type:

bash CreateSQLFilesStreams.sh 1000

To create the SQL files corresponding to the 1000 GB (or 1 TB) scale factor. Although the create table 
statements do not change with the scale factor, some of the attribute values used in the queries do change.
The generated files will be stored inside the tpcdsbench/vols/data folder, but they are also copied into
the tpcdsbench/client/project/src/main/resources directory, and from this location added to the compiled
jar. 

Once the SQL files have been generated, the benchmarking application can be compiled, which will include
them. Separate jar files are generated for Presto and Spark. In the case of Spark, the built application
works on EMR as well as on Databricks.

3.1) Compile the JDBC application

Within the tpcdsbench directory, type

bash compileClient.sh 0

The script above will create a file named client-1.2-SNAPSHOT-SHADED.jar that will also be copied into
the $HOME/tpcds-jars/targetemr/ directory. It is important to note that the tpcds-jars directory is a 
mounted s3 bucket, so in effect any previous version of the jar file stored in s3 will be overwritten.
The 0 parameter is to generate a file name without a timestamp.

2.2) Compile the Spark application

Within the tpcdsbench directory, type

bash compileClientDatabricks.sh 0

The process is analogous to the Presto application. Again a file named client-1.2-SNAPSHOT-SHADED.jar will
be generated but in this case it will be stored in $HOME/tpcds-jars/targetsparkdatabricks/. The value 0
indicates to generate the file with the name just mentioned, whereas a value of 1 will indicate to append
a timestamp to the jar file, in order to avoid overwriting the existing file, if any.

Different systems require different scripts and procedures in order to run experiments, which are detailed in
the next sections. In all cases, however, the execution of the application jar stored in the tpcds-jars s3 bucket
produces experimental results that are stored in the tpcds-results-test s3 bucket, using the locations specified
in the arguments of the application, which in turn are specified within the script used to run the experiment.

3) Generate the data

It is possible to use the TPC-DS Toolkit container to generate data by relying on its data generator.
For a small scale factor (e.g. 1 or 10 GB), it suffices to have enough disk space and use the command

bash createDataFiles.sh <scale factor>

The data is stored in the directory $HOME/tpcdsbench/vols/hive/<scale factor>GB


RUNNING EXPERIMENTS IN EMR SPARK

IMPORTANT NOTE: before running experiments with EMR, it is necessary to properly configure the AWS CLI.

The scripts enabling running experiments in EMR Spark are inside the $HOME/tpcdsbench/emr directory. There are
four main modalities of running experiments:

1) Create a single cluster configured to run the benchmarking application automatically as a step.
The execution represents a single instance of a specific experiment.

2) Create multiple clusters simultaneously configured to run the benchmarking application automatically
as a step. The execution represents multiple instances of a specific experiment.

3) Add a step to a running cluster created previously, in order to run the benchmarking application.
This can be useful for development and testing.

4) Run the application using spark-submit on a running cluster.

It is important to clarify that an experiment is identified by its name, while its instances are identified
by integer numbers. Uniqueness in the experiment name is currently achievable by incorporating a timestamp.
Therefore, in order to use option 1 to create additional instances of an experiment it is necessary to ensure
that the script will use the same experiment name, including the timestamp if it was used.

We describe each of these options next, in every case it is necessary to have the AWS CLI installed and properly
configured. In many cases, an alternative scala script is provided in addition to the corresponding bash script.

1) Create a single cluster:

Run the script tpcdsbench/emr/runclient_emrspark_createwithstep.sh as follows

bash emr/runclient_emrspark_createwithstep.sh <scale factor> <experiment instance number> <number of streams>

As described, the script expects the scale factor (e.g. 1000 GB), the instance number of the experiment (e.g. 1)
and the number of streams for the power test (e.g. 4). A series of variables within the script define additional
properties for the experiment, such as its name, the data location of the raw and generated column-storage data, 
and the name of the database to be created.

Based on the variables above, a list of arguments is defined for the benchmarking java application, which also
includes the location of metrics files, query results, query plans, and the generated additional SQL statements.
In addition, these arguments specify whether to use data partitioning, use column statistics, etc. It is also
possible to specify via a flags argument which specific tests should be performed (among load, analyze, power,
and tput).

The parameters of the cluster to be used, such as the number of nodes and the EMR version are also present in
the script, some as script variables and others within JSON "here documents" forming part of functions. These
functions specify the Spark configuration, the bootstrap script to use (which is essential for the operation
of the benchmark application), and the security groups for the cluster, among other settings.

The script essentially takes the input parameters, the values of script variables, and the values generated
by the functions to create an AWS CLI command that creates the cluster with the step to run the benchmarking
application with the appropriate parameters. It is also possible to configure the script to wait for the
termination of the application and with it the cluster in order to perform tasks such as deleting the
generated column-storage files, if so desired.

An equivalent script is provided in Scala, which can be copied into a Databricks Notebook and executed in 
a similar manner. The only requirement is that the cluster used to run the script must have the AWS CLI
installed and properly configured, as well as adding a json processing library whose coordinates are specified
in comments within the script. This script is located in:


tpcdsbench\emr\scala\runclient_emrspark_createwithstep.scala


2) Create multiple clusters simultaneously.

In this case multiple clusters to run the application with the same parameters will be created, which together
represent multiple instances of a single experiment. The script is located in 
tpcdsbench/emr/runclient_emrspark_createwithstep_multiple.sh and executed using

bash runclient_emrspark_createwithstep_multiple.sh <scale factor> <number of experiment instances> <number of streams>

The mode of operation of this script is very similar to the script for a single cluster in option 1. An important
difference is that the second parameter now specifies the total number of experiment instances to run, instead of
the number of the single instance to be executed.

Again, a scala version of this script is provided that can be incorporated into a Databricks notebook:

tpcdsbench/emr/scala/runclient_emrspark_createwithstep_multiple.scala


3) Add a step to a running cluster.

This option is useful for development and testing, since it is not necessary to create a new cluster each time.
The initial cluster can be created with the script for option 1, setting the flags to just the tasks required,
such as creating the database and analyzing the data.

In order to use this script it is necessary to specify the cluster id of the running cluster in its corresponding
variable within the script. If the script for option 1 was used to create the cluster, its id will be printed
during the creation of the cluster, otherwise it can be found in the AWS EMR Web GUI.

The operation and execution of this script is very similar to that of option 1, namely

bash runclient_emrspark_addstep.sh <scale factor> <experiment instance number> <number of streams>

Therefore the main difference is that the cluster is already running and consequently its configuration
does not need to be specified by this script. The equivalent script in scala can be found in:

tpcdsbench/emr/scala/runclient_emrspark_addstep.scala

4) Use spark-submit on a running cluster.

This alternative can be useful for development and debugging. It is faster than adding a step but does not have
the benefits in auditing of EMR steps, since logs and parameters used are not directly accessible from the GUI.

It is not necessary to download all the benchmarking platform in the running cluster, it suffices to upload the
script: 

tpcdsbench/emr/runclient_fullbenchmark_sparksubmit.sh

which has the same interface as the single cluster option, i.e.,

bash runclient_fullbenchmark_sparksubmit.sh <scale factor> <experiment instance number> <number of streams>



RUNNING EXPERIMENTS IN DATABRICKS

The scripts enabling running experiments in Databricks are inside the $HOME/tpcdsbench/databricks directory.
Overall, the operation of the scripts and their execution is very similar to the analogous case for EMR Spark.
There are two main modalities of running experiments:

1) Create a single job configured to run the benchmarking application with a new cluster.
The execution represents a single instance of a specific experiment.

2) Create multiple jobs simultaneously configured to run the benchmarking application with new clusters.
The execution represents multiple instances of a specific experiment.

We describe each of these options next, in every case it is necessary to have the Databricks CLI installed and
properly configured. In both cases, an alternative scala script is provided in addition to the corresponding 
bash script.

1) Create a single job.

In this case a job is created to run the benchmarking application with the appropriate configuration parameters,
on a new cluster whose configuration is also generated by the script. The script is located in 
tpcdsbench/databricks/runclient_dbr_job.sh and executed with

bash databricks/runclient_dbr_job.sh <scale factor> <experiment instance number> <number of streams>

As is the case with EMR Spark, the second parameter specifies a single instance for the experiment.
Variables and a here document within the script enable the specification of the job and the associated
new cluster configuration. It is possible to not only create the job but also trigger its execution by
setting the appropriate variables within the script.

In addition, it is possible to wait for the termination of the run of the job to execute additional tasks
such as deleting the generated column-storage files. Finally, the script allows to use the Databricks REST
API instead of the Databricks CLI, by modifying the corresponding variable and setting the DATABRICKS_TOKEN
environment variable to the token used for databricks authentication and authorization.

The equivalent script in scala is located in:

tpcdsbench/databricks/scala/runclient_dbr_job.scala


2) Create multiple jobs simultaneously.

For this option multiple jobs are created, each of which represents a specific instance of a single
experiment, all using the same application parameters and cluster configuration. As is the case for
option 1, it is possible to also trigger the execution of the jobs in addition to their creation by setting
the corresponding variable in the script. The script is located in 
tpcdsbench/databricks/runclient_dbr_job_multiple.sh and can be executed with the command

bash runclient_dbr_job_multiple.sh <scale factor> <number of experiment instances> <number of streams>

The equivalent script in scala is located in

tpcdsbench/databricks/scala/runclient_dbr_job_multiple.scala







 
















