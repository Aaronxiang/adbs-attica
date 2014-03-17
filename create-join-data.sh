#!/bin/bash

function createTuple {
	echo -e "insert into $1 values ($2);"
}

function dropTable {
	echo "drop table $1;"
}

function createTable {
	echo "create table $1 ($2);"
}

function generateReplicatedString {
	baseString=$1;
	let repsM1=$2-1;
	resString="";
	for i in $(seq 0 $repsM1); do
		resString="${resString}${baseString}"
	done; 
	echo ${resString}
}

function incrementChars {
	chars=""; 
    testchars=$1;
    while test -n "$testchars"; do
        c=${testchars:0:1}     # Get the first character
        if [ "${c}_" == "__" ]; then
            c="a";
        fi;
        testchars=${testchars:1}   # trim the first character
        chars=${chars}${c}
    done
	res=$(echo "$chars" | tr "0-9a-z" "1-9a-z_"); 
	echo "$res"
}

if [ ! $# -eq 3 ]; then
	echo "Usage: $0 <tuples-no> <group-size> <table-name>"
	exit 1;
fi;

tuplesNo=$1;
groupSize=$2;
tableName=$3;

i=0;
letters="aaaaaaa"
groupId=0;
groupCntr=0;

dropTable $tableName;
createTable $tableName "vInt integer, gInt integer, gStr string";

while [ $i -lt $tuplesNo ]; do 
	let i=$i+1;
	let groupCntr=$groupCntr+1;

	createTuple $tableName "$i, $groupId, '${letters}'";

	if [ $groupCntr -eq $groupSize ]; then
		groupCntr=0;
		let groupId=$groupId+1;
		letters=$(incrementChars ${letters});
	fi;	
done;

echo "exit;"


#time echo "select sm_10_4.vInt, sm_10_4.gStr, sm_10_2.vInt, sm_10_2.gStr from sm_10_4, sm_10_2 where sm_10_4.gInt=sm_10_2.gInt; exit;" | ../runAttica.sh

#time echo "select sm_10000_10.vInt, sm_10000_10.gStr, sm_10000_20.vInt, sm_10000_20.gStr from sm_10000_10, sm_10000_20 where sm_10000_10.gInt=sm_10000_20.gInt; exit;" | ../runAttica.sh
