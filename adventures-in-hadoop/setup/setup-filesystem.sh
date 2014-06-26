#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f ${0}))
PATH=$PATH:$HADOOP_HOME/bin

pushd $SCRIPT_DIR

hadoop fs -rm -r /tmp/adventures-in-hadoop

hadoop fs -mkdir -p \
	/tmp/adventures-in-hadoop/input \
	/tmp/adventures-in-hadoop/map-reduce/inverted-index/output

hadoop fs -put $SCRIPT_DIR/data/* /tmp/adventures-in-hadoop/input
