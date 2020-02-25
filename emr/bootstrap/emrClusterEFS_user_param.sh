#!/bin/bash

#$1 username to own the directory

sudo yum install -y amazon-efs-utils
file_system_id_01=fs-63cc8fc8
efs_directory=/mnt/efs

sudo mkdir -p ${efs_directory}
#Cannot append to file directly with echo and sudo.
echo "${file_system_id_01}:/ ${efs_directory} efs tls,_netdev" | sudo tee -a /etc/fstab
sudo mount -a -t efs defaults
if [ ! -d ${efs_directory}/scratch/$1/data ]; then
	sudo mkdir -p ${efs_directory}/scratch/$1/data
	sudo chown $1:$1 -R ${efs_directory}/scratch/$1
fi
if [ ! -d /scratch/$1/data/ ]; then
	sudo mkdir -p /scratch/$1/data/
	sudo chown $1:$1 -R /scratch/$1
fi
if [ ! -d /data ]; then
	sudo mkdir -p /data
	sudo chown $1:$1 -R /data
fi

