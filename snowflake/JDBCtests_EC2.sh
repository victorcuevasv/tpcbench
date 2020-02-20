#!/bin/bash 

#Obtain json data
#aws ec2 get-launch-template-data --instance-id "i-0d412d147e820bc7c" --query "LaunchTemplateData" > ec2JSON.txt

json_data_func()
{
  cat <<EOF
{
    "UserData": "IyEvYmluL2Jhc2gKCnN1ZG8geXVtIGluc3RhbGwgLXkgYW1hem9uLWVmcy11dGlscwpmaWxlX3N5c3RlbV9pZF8wMT1mcy02M2NjOGZjOAplZnNfZGlyZWN0b3J5PS9tbnQvZWZzCgpzdWRvIG1rZGlyIC1wICR7ZWZzX2RpcmVjdG9yeX0KI0Nhbm5vdCBhcHBlbmQgdG8gZmlsZSBkaXJlY3RseSB3aXRoIGVjaG8gYW5kIHN1ZG8uCmVjaG8gIiR7ZmlsZV9zeXN0ZW1faWRfMDF9Oi8gJHtlZnNfZGlyZWN0b3J5fSBlZnMgdGxzLF9uZXRkZXYiIHwgc3VkbyB0ZWUgLWEgL2V0Yy9mc3RhYgpzdWRvIG1vdW50IC1hIC10IGVmcyBkZWZhdWx0cwppZiBbICEgLWQgJHtlZnNfZGlyZWN0b3J5fS9zY3JhdGNoL2RhdGEgXTsgdGhlbgoJc3VkbyBta2RpciAtcCAke2Vmc19kaXJlY3Rvcnl9L3NjcmF0Y2gvZGF0YQoJc3VkbyBjaG93biBlYzItdXNlcjplYzItdXNlciAtUiAke2Vmc19kaXJlY3Rvcnl9L3NjcmF0Y2gKCXN1ZG8gbWtkaXIgLXAgL3NjcmF0Y2gvZGF0YQoJc3VkbyBjaG93biBlYzItdXNlcjplYzItdXNlciAtUiAvc2NyYXRjaApmaQppZiBbICEgLWQgL3NjcmF0Y2gvZGF0YSBdOyB0aGVuCglzdWRvIG1rZGlyIC1wIC9zY3JhdGNoL2RhdGEKCXN1ZG8gY2hvd24gZWMyLXVzZXI6ZWMyLXVzZXIgLVIgL3NjcmF0Y2gKZmkKCnN1ZG8geXVtIGluc3RhbGwgZG9ja2VyIC15CnN1ZG8gdXNlcm1vZCAtYUcgZG9ja2VyIGVjMi11c2VyCnN1ZG8gc2VydmljZSBkb2NrZXIgcmVzdGFydAo=", 
    "Monitoring": {
        "Enabled": false
    },
    "TagSpecifications": [
        {
            "ResourceType": "instance", 
            "Tags": [
                {
                    "Value": "JDBCtests", 
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
--launch-template-name JDBCtestsTemplate \
--version-description version1 \
--launch-template-data $SINGLE_LINE_JSON

 
 
