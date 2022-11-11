#!/bin/bash 

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 Name of the template

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash create_vm_templateEC2_qbeast.sh <template name>${end}"
    exit 0
fi

#Obtain json data
#aws ec2 get-launch-template-data --instance-id "i-0d412d147e820bc7c" --query "LaunchTemplateData" > ec2JSON.txt

SecurityGroupId="sg-014e2eff525ef62c5"
RoleWithPolicyArn="arn:aws:iam::834198847803:instance-profile/EMR_EC2_Benchmarking"
KeyPair="devtest_qbeast_nvirginia_keypair"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

encodedScript=$(base64 --wrap=0 $DIR/s3fs_init_qbeast.sh)

json_data_func()
{
  cat <<EOF
{
    "UserData": "$encodedScript", 
    "Monitoring": {
        "Enabled": false
    },
    "TagSpecifications": [
        {
            "ResourceType": "instance", 
            "Tags": [
                {
                    "Value": "BenchmarkVM", 
                    "Key": "Name"
                }
            ]
        }
    ], 
    "CapacityReservationSpecification": {
        "CapacityReservationPreference": "open"
    }, 
    "InstanceInitiatedShutdownBehavior": "stop", 
    "ImageId": "ami-026b57f3c383c2eec", 
    "BlockDeviceMappings": [
        {
            "DeviceName": "/dev/xvda", 
            "Ebs": {
                "Encrypted": false, 
                "DeleteOnTermination": true, 
                "VolumeType": "gp2", 
                "VolumeSize": 20
            }
        }
    ], 
    "KeyName": "$KeyPair", 
    "CreditSpecification": {
        "CpuCredits": "standard"
    }, 
    "HibernationOptions": {
        "Configured": false
    }, 
    "EbsOptimized": false, 
    "Placement": {
        "Tenancy": "default", 
        "GroupName": "", 
        "AvailabilityZone": "us-east-1b"
    }, 
    "IamInstanceProfile": {
        "Arn": "$RoleWithPolicyArn"
    },
    "DisableApiTermination": false, 
    "InstanceType": "t2.medium", 
    "NetworkInterfaces":[ 
      { 
         "DeviceIndex":0,
         "AssociatePublicIpAddress":true,
         "Groups":[ 
            "$SecurityGroupId"
         ],
         "DeleteOnTermination":true,
         "SubnetId": "subnet-02b07eb9167614b28"
      }
   ]
}
EOF
}

#Create the template
aws --profile benchmarking ec2 create-launch-template \
--launch-template-name BenchmarkDataGenVM \
--version-description version1 \
--launch-template-data "$(json_data_func)"

 
 
