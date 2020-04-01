#!/bin/bash

#User to own the directories.
mUSER="ec2-user"

#Install s3fs
sudo yum install -y gcc libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel git
git clone https://github.com/s3fs-fuse/s3fs-fuse
(cd s3fs-fuse && ./autogen.sh)
(cd s3fs-fuse && ./configure --prefix=/usr --with-openssl)
make -C ./s3fs-fuse
sudo make -C ./s3fs-fuse install

#Mount the bucket
mkdir /home/$mUSER/s3-drive
sudo chown $mUSER:$mUSER -R /home/$mUSER/s3-drive
sudo -u $mUSER s3fs -o iam_role="tpcds-mount" -o url="https://s3-us-west-2.amazonaws.com" \
-o endpoint=us-west-2 -o dbglevel=info -o curldbg \
-o use_cache=/tmp tpcds-temporal /home/$mUSER/s3-drive
