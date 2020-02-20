#!/bin/bash 

#Obtain json data
#aws ec2 get-launch-template-data --instance-id "i-0d412d147e820bc7c" > ec2JSON.txt

json_data_func()
{
  cat <<EOF
{ 
   "NetworkInterfaces":[ 
      { 
         "AssociatePublicIpAddress":true,
         "DeviceIndex":0,
         "Ipv6AddressCount":1,
         "SubnetId":"subnet-7b16de0c"
      }
   ],
   "ImageId":"ami-8c1be5f6",
   "InstanceType":"t2.small",
   "TagSpecifications":[ 
      { 
         "ResourceType":"instance",
         "Tags":[ 
            { 
               "Key":"purpose",
               "Value":"webserver"
            }
         ]
      }
   ]
}
EOF
}

SINGLE_LINE_JSON=$(jq -c . <<<  "$(json_data_func)")

#Create the template
aws ec2 create-launch-template \
--launch-template-name TestTemplate \
--version-description version1 \
--launch-template-data $SINGLE_LINE_JSON

 
 
