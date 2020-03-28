#!/bin/bash

#$1 username to own the directory
#$2 AWS_ACCESS_KEY
#$3 AWS_SECRET_KEY

sudo yum install -y gcc libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel git
git clone https://github.com/s3fs-fuse/s3fs-fuse
(cd s3fs-fuse && ./autogen.sh)
(cd s3fs-fuse && ./configure --prefix=/usr --with-openssl)
make -C ./s3fs-fuse
sudo make -C ./s3fs-fuse install
echo $2:$3 > /home/$1/.passwd-s3fs
chmod 600 /home/$1/.passwd-s3fs
sudo chown $1:$1 /home/$1/.passwd-s3fs
mkdir /home/$1/s3-drive
sudo chown $1:$1 -R /home/$1/s3-drive
sudo -u username "s3fs tpcds-temporal /home/$1/s3-drive"

