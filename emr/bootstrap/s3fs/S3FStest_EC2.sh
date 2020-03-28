#!/bin/bash 

#Obtain json data
#aws ec2 get-launch-template-data --instance-id "i-0d412d147e820bc7c" --query "LaunchTemplateData" > ec2JSON.txt

encodedScript=$(cat s3fs_init.sh | base64)

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
                    "Value": "TestMachine", 
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
                "VolumeSize": 10, 
                "SnapshotId": "snap-0111a56dd4bda22e9"
            }
        }
    ], 
    "KeyName": "testalojakeypair", 
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
    "DisableApiTermination": false, 
    "InstanceType": "t2.xlarge", 
    "NetworkInterfaces":[ 
      { 
         "DeviceIndex":0,
         "AssociatePublicIpAddress":true,
         "Groups":[ 
            "sg-024ebdd0a54593a56",
            "sg-e9663ea1"
         ],
         "DeleteOnTermination":true,
         "SubnetId": "subnet-01033078"
      }
   ]
}
EOF
}

SINGLE_LINE_JSON=$(jq -c . <<<  "$(json_data_func)")

#Create the template
aws ec2 create-launch-template \
--launch-template-name JDBCtests \
--version-description version1 \
--launch-template-data $SINGLE_LINE_JSON

 
 
