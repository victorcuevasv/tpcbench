#!/bin/bash

#Install git and docker

sudo yum install git -y
sudo yum install docker -y
sudo usermod -aG docker ec2-user
sudo service docker restart

