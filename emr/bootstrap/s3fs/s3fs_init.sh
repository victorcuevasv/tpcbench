#!/bin/bash

#$1 username to own the directory

#Install s3fs
sudo yum install -y gcc libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel git
git clone https://github.com/s3fs-fuse/s3fs-fuse
(cd s3fs-fuse && ./autogen.sh)
(cd s3fs-fuse && ./configure --prefix=/usr --with-openssl)
make -C ./s3fs-fuse
sudo make -C ./s3fs-fuse install

#Mount the bucket
mkdir /home/$1/s3-drive
sudo chown $1:$1 -R /home/$1/s3-drive
sudo -u $1 s3fs -o iam_role="tpcds-mount" -o url="https://s3-us-west-2.amazonaws.com" \
-o endpoint=us-west-2 -o dbglevel=info -o curldbg -o allow_other \
-o use_cache=/tmp tpcds-temporal /home/$1/s3-drive
