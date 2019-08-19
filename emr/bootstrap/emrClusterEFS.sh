#!/bin/bash

sudo yum install -y amazon-efs-utils
file_system_id_01=fs-63cc8fc8
efs_directory=/mnt/efs

sudo mkdir -p ${efs_directory}
sudo echo "${file_system_id_01}:/ ${efs_directory} efs tls,_netdev" >> /etc/fstab
sudo mount -a -t efs defaults
sudo mkdir ${efs_directory}/data
sudo chmod -R 777 ${efs_directory}/data

 