
INSTALLATION INSTRUCTIONS

It is recommended to use a VM on AWS to run the experiments for ease of use, reproducibility of results, 
and to minimize network latency for some experiments. However, it is possible to run the experiments on any 
machine after completing the equivalent tasks performed by the AWS initialization script.

1) Download the code to a machine with the AWS CLI installed and properly configured.

git clone https://victorcuevasv@bitbucket.org/victorcuevasv/tpcdsbench.git

2) Create a virtual machine on AWS.

Scripts are provided to create a VM on AWS with the necessary configuration. If the artifacts created by
some of the scripts already exist in the AWS account, the corresponding steps can be omitted.

2.1) Create a security group enabling ssh access to the VM (can be skipped if it already exists).

Enter the tpcdsbench directory and execute the create_security_group_EC2.sh script. The security group
will be created with an inbound rule to enable ssh access. The script should also output the id of the
security group, which will be needed later.

bash emr/bootstrap/s3fs/create_security_group_EC2.sh

2.2) Create an EC2 instance template. The script create_vm_templateEC2.sh enables to do this. It requires
3 configuration values.

-line 6: the id of the security group to enable ssh access, resulting from the previous step.
-line 7: the arn of a role enabling access to the s3 buckets, discussed later.
-line 8: the name of the AWS keypair to use for ssh access 

An IAM role has to be created to enable access to the s3 buckets using s3fs. The role can be created via the
AWS web GUI, along with a new policy that can be specified in the form

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListAllMyBuckets"
            ],
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::tpcds-jars",
                "arn:aws:s3:::tpcds-results-test"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:PutObjectAcl"
            ],
            "Resource": [
                "arn:aws:s3:::tpcds-jars/*",
                "arn:aws:s3:::tpcds-results-test/*"
            ]
        }
    ]
}

The buckets currently in use are tpcds-jars and tpcds-results-test. The IAM role is identified by the 
arn but also by its name, which is used in the initialization script. Once the 3 values mentioned above
are present in the create_vm_templateEC2.sh, it is also necessary to modify the initialization script
s3fs_init.sh located in the same directory, concretely

-line 6 (s3fs_init.sh): the name of the IAM role used to access the s3 buckets.

When the script create_vm_templateEC2.sh is executed, it will encode the s3fs_init.sh script in Base64,
so that it can be sent as part of the AWS CLI request to create the template. Consequently, to create
the template it suffices to execute the create_vm_templateEC2.sh script as follows

bash emr/bootstrap/s3fs/create_vm_templateEC2.sh

The execution of the script results in the creation of an EC2 instance template named BenchmarkVM. When
a virtual machine instance is created from this template, the s3fs_init.sh script will be executed, which
will install s3fs and mount the tpcds-jars and tpcds-results-test buckets. In addition, it will install
git, docker, jq, and the databricks-cli.

The VM can be created directly from the AWS web GUI (EC2 > Launch templates > Launch instance from template).
The benchmark system can then be downloaded to the newly created VM in order to run benchmarking experiments,
as it was done in step 1.

git clone https://victorcuevasv@bitbucket.org/victorcuevasv/tpcdsbench.git


3) Build the benchmarking infrastructure.

The building process will create the container images to generate the queries, compile the application,
and run experiments. The instructions in this section cover Databricks, EMR Spark, and EMR Presto.
However, in the case of Databricks JDBC, additional steps are required due to the JDBC driver used.
The additional steps to run Databricks JDBC are detailed later in this document.

3.1) Create the containers.

The BuildAll.sh script creates the necessary container images, it can be run with the instruction.

bash emr/BuildAll.sh

The container images created shoud include: clientbuilder, tpcds, ubuntujava, and ubuntu. The clientbuilder
image contains the benchmarking application code, while the tpcds image contains the data and queries
generation tools adapted from the TPC-DS Toolkit.


RUNNING EXPERIMENTS

After the benchmarking infrastructure has been installed, in order to run experiments it is still necessary
to generate the SQL files (create table statements and queries) and to compile the benchmarking application.
It is important to remark that the benchmarking application and the SQL files it will run are encapsulated
in the same jar file. However, the generation of the queries and the compilation of the application are 
separate tasks.

1) Create the SQL files

It is absolutely necessary to create the SQL files before compiling the application, otherwise the SQL files
will not be included in the jar. Inside the tpcdsbench directory, to create the SQL files execute the following
script, supplying the scale factor (as an integer denoting gigabytes):

bash CreateSQLFiles.sh <scale factor>

For example, type:

bash CreateSQLFiles.sh 1000

To create the SQL files corresponding to the 1000 GB (or 1 TB) scale factor. Although the create table 
statements do not change with the scale factor, some of the attribute values used in the queries do change.

2) Compile the benchmarking application

Once the SQL files have been generated, the benchmarking application can be compiled, which will include
them. Separate jar files are generated for Presto and Spark. In the case of Spark, the built application
works on EMR as well as on Databricks.

2.1) Compile the Presto application

Within the tpcdsbench directory, type

bash compileclientEMR.sh

The script above will create a file named client-1.2-SNAPSHOT-SHADED.jar that will also be copied into
the $HOME/tpcds-jars/targetemr/ directory. It is important to note that the tpcds-jars directory is a 
mounted s3 bucket, so in effect any previous version of the jar file stored in s3 will be overwritten.

2.2) Compile the Spark application

Within the tpcdsbench directory, type

bash compileclientSparkDatabricks.sh

The process is analogous to the Presto application. Again a file named client-1.2-SNAPSHOT-SHADED.jar will
be generated but in this case it will be stored in $HOME/tpcds-jars/targetsparkdatabricks/





