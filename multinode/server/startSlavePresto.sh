#!/bin/bash

#Start the ssh server.
/etc/init.d/ssh start

/opt/presto-server-0.214/bin/launcher run

#sleep infinity

