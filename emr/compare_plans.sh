#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"


if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash diff_plans.sh <plans dir 1> <plans dir 2> <output dir>${end}"
    exit 0
fi

#Count the number of nodes in corresponding plans.
#Convert the graphviz output to plain (redirecting errors to /dev/null),
#then filter the node names and finally count those lines.

for i in {1..99} ;
do
	n1=$(dot -Tplain $1/query${i}.txt 2>/dev/null  | sed -ne 's/^node \([^ ]\+\).*$/\1/p' | wc -l)
	n2=$(dot -Tplain $2/query${i}.txt 2>/dev/null  | sed -ne 's/^node \([^ ]\+\).*$/\1/p' | wc -l)
	if [[ $n1 != $n2 ]] ; then
		printf "%s,%s,%s\n" $i $n1 $n2
	fi
done

#Install graphviz in EC2 machine
#Install C compiler and additional development tools.
#sudo yum groupinstall "Development tools"
#Additional dependencies for pdf format.
#sudo yum install cairo-devel pango-devel
#Download, compile and install.
##wget https://www2.graphviz.org/Packages/stable/portable_source/graphviz-2.44.0.tar.gz
##tar -xvf graphviz-2.44.0.tar.gz
##cd graphviz-2.44.0
#./configure 
#make
#make install 
##sudo make install


#Obtain png graphviz visualization of the plan
#dot -Tpng query1.txt > query1.png


