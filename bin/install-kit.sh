#!/usr/bin/env bash

export SPARK_HOME=${SPARK_HOME:-${1}}

sudo yum install gcc
sudo yum install bison
sudo yum install byacc
sudo yum install flex

cd ${SPARK_HOME}
git clone https://github.com/peisun1115/tpcds-kit kit
cd kit/tools
cp Makefile.suite Makefile
make
cd ${SPARK_HOME}

for i in $(cat ${SPARK_HOME}/conf/slaves); do 
    scp -r kit ${i}:${SPARK_HOME}/kit
done
