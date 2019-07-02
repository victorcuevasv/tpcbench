#!/bin/bash

#Create the folder for the logs and results.

if [ ! -d /data ]; then
	sudo mkdir /data
	sudo chmod -R 777 /data
fi

