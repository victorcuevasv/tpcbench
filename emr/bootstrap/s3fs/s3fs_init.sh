#!/bin/bash

#User to own the directories.
mUSER="ec2-user"
#Buckets to mount. It is assumed an IAM role enabling access to them is associated with the machine.
buckets=("tpcds-jars" "tpcds-results-test")

#Install s3fs.
sudo yum install -y gcc libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel git
git clone https://github.com/s3fs-fuse/s3fs-fuse
(cd s3fs-fuse && ./autogen.sh)
(cd s3fs-fuse && ./configure --prefix=/usr --with-openssl)
make -C ./s3fs-fuse
sudo make -C ./s3fs-fuse install

#Mount the buckets.
for bucket in "${buckets[@]}" ; do
	mkdir /home/$mUSER/$bucket
	sudo chown $mUSER:$mUSER -R /home/$mUSER/$bucket
	sudo -u $mUSER s3fs -o iam_role="tpcds-mount" -o url="https://s3-us-west-2.amazonaws.com" \
	-o endpoint=us-west-2 -o dbglevel=info -o curldbg \
	-o use_cache=/tmp $bucket /home/$mUSER/$bucket
done

#Install docker.
sudo yum install docker -y
sudo usermod -aG docker ec2-user
sudo service docker restart

