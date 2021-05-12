#!/bin/bash

#$1 username to own the directory
#$2 buckets to mount as comma separated list

sudo yum install -y gcc libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel git
git clone https://github.com/s3fs-fuse/s3fs-fuse
(cd s3fs-fuse && ./autogen.sh)
(cd s3fs-fuse && ./configure --prefix=/usr --with-openssl)
make -C ./s3fs-fuse
sudo make -C ./s3fs-fuse install

#Get the list of buckets as an array, replace the commas by spaces
#and then interpret the string as an array.
buckets=(${2//,/ })
#Mount the buckets.
#Use the option allow_other, which requires uncommenting the line of the file /etc/fuse.conf
#that contains user_allow_other with sed
sudo sed -i '/user_allow_other/s/^#//g' /etc/fuse.conf
for bucket in "${buckets[@]}" ; do
	mkdir /mnt/$bucket
	sudo chown $1:$1 -R /mnt/$bucket
	sudo -u $1 s3fs -o iam_role="EMR_EC2_DefaultRole" -o url="https://s3-us-west-2.amazonaws.com" \
	-o endpoint=us-west-2 -o dbglevel=info -o curldbg \
	-o use_cache=/tmp -o allow_other $bucket /mnt/$bucket
done

if [ ! -d /data ]; then
	sudo mkdir -p /data
	sudo chown $1:$1 -R /data
fi

#Copy the org.json jar
sleep 15
sudo mkdir -p /home/hadoop/extrajars/
sudo cp /mnt/tpcds-jars/orgjson/json-20210307.jar /home/hadoop/extrajars/
sudo chmod 755 /home/hadoop/extrajars/json-20210307.jar


