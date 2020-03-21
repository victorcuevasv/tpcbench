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
	printf "%s,%s,%s\n" $i $n1 $n2
done

