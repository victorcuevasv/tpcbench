
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



