#!/bin/bash

#User to own the directories.
mUSER="ec2-user"
#Name of the iam role with the policy enabling access to the s3 buckets.
iamRoleName="EMR_EC2_Benchmarking_ROLE"
#Buckets to mount. It is assumed an IAM role enabling access to them is associated with the machine.
buckets=("benchmarking-jars-1664735376" "benchmarking-results-1664735376")

#Install s3fs.
sudo yum install -y gcc libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel git
sudo amazon-linux-extras install epel -y
sudo yum install s3fs-fuse -y

#Mount the buckets.
for bucket in "${buckets[@]}" ; do
	mkdir /home/$mUSER/$bucket
	sudo chown $mUSER:$mUSER -R /home/$mUSER/$bucket
	sudo -u $mUSER s3fs -o iam_role="$iamRoleName" -o url="https://s3.us-east-1.amazonaws.com" \
	-o endpoint=us-east-1 -o dbglevel=info -o curldbg \
	-o use_cache=/tmp $bucket /home/$mUSER/$bucket
done

#Install docker.
sudo yum install docker -y
sudo usermod -aG docker ec2-user
sudo service docker restart

#Install jq
sudo yum install jq -y

#Install Databricks CLI
sudo curl -O https://bootstrap.pypa.io/get-pip.py
sudo -u ec2-user python get-pip.py --user
sudo rm get-pip.py
sudo -u ec2-user /home/ec2-user/.local/bin/pip install databricks-cli

#Install Github CLI
sudo yum-config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
sudo yum install gh -y

#Install OpenJDK
sudo amazon-linux-extras install java-openjdk11 -y

