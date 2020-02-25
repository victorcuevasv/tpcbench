#!/bin/bash

sudo yum install -y amazon-efs-utils
file_system_id_01=fs-63cc8fc8
efs_directory=/mnt/efs

sudo mkdir -p ${efs_directory}
#Cannot append to file directly with echo and sudo.
echo "${file_system_id_01}:/ ${efs_directory} efs tls,_netdev" | sudo tee -a /etc/fstab
sudo mount -a -t efs defaults
if [ ! -d ${efs_directory}/scratch/data ]; then
	sudo mkdir -p ${efs_directory}/scratch/data
	sudo chown ec2-user:ec2-user -R ${efs_directory}/scratch
	sudo mkdir -p /scratch/data
	sudo chown ec2-user:ec2-user -R /scratch
fi
if [ ! -d /scratch/data ]; then
	sudo mkdir -p /scratch/data
	sudo chown ec2-user:ec2-user -R /scratch
fi

sudo yum install git -y
sudo yum install jq -y
sudo yum install docker -y
sudo usermod -aG docker ec2-user
sudo service docker restart

sudo curl -O https://bootstrap.pypa.io/get-pip.py
sudo -u ec2-user python get-pip.py --user
sudo rm get-pip.py
sudo -u ec2-user /home/ec2-user/.local/bin/pip install databricks-cli

