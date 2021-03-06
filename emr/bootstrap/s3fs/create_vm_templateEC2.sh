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
    echo "${yel}Usage: bash create_vm_templateEC2.sh <template name>${end}"
    exit 0
fi

#Obtain json data
#aws ec2 get-launch-template-data --instance-id "i-0d412d147e820bc7c" --query "LaunchTemplateData" > ec2JSON.txt

SecurityGroupId="sg-0d676074919b90fac"
RoleWithPolicyArn="arn:aws:iam::384416317380:instance-profile/tpcds-mount"
KeyPair="testalojakeypair"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

encodedScript=$(base64 --wrap=0 $DIR/s3fs_init.sh)

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
    "ImageId": "ami-0e8c04af2729ff1bb", 
    "BlockDeviceMappings": [
        {
            "DeviceName": "/dev/xvda", 
            "Ebs": {
                "Encrypted": false, 
                "DeleteOnTermination": true, 
                "VolumeType": "gp2", 
                "VolumeSize": 20, 
                "SnapshotId": "snap-0111a56dd4bda22e9"
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
        "AvailabilityZone": "us-west-2b"
    }, 
    "IamInstanceProfile": {
        "Arn": "$RoleWithPolicyArn"
    },
    "DisableApiTermination": false, 
    "InstanceType": "t2.xlarge", 
    "NetworkInterfaces":[ 
      { 
         "DeviceIndex":0,
         "AssociatePublicIpAddress":true,
         "Groups":[ 
            "$SecurityGroupId"
         ],
         "DeleteOnTermination":true,
         "SubnetId": "subnet-01033078"
      }
   ]
}
EOF
}

#Create the template
aws ec2 create-launch-template \
--launch-template-name BenchmarkVM \
--version-description version1 \
--launch-template-data "$(json_data_func)"

 
 
