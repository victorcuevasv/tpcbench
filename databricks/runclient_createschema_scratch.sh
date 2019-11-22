#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_createschema_scratch.sh <scale factor> <instance> <tag>${end}"
    exit 0
fi

BUCKET_NAME=tpcds-warehouse-databricks-$1gb-$2-$3
MOUNT_NAME=tpcdswarehousebucket$1GB_$2_$3
DATABASE_NAME=tpcdsdb$1gb_$2_$3

#Create a the bucket for the database.

aws s3api create-bucket --bucket $BUCKET_NAME --region us-west-2 \
 --create-bucket-configuration LocationConstraint=us-west-2 --acl private
 
#Create an empty directory within the bucket.
 
aws s3api put-object --bucket $BUCKET_NAME --key empty

#Mount the bucket and create the database.

databricks jobs run-now --job-id 119 --notebook-params \
'{"BucketName":"'"$BUCKET_NAME"'", "MountName":"'"$MOUNT_NAME"'", "DatabaseName":"'"$DATABASE_NAME"'"}'


  


