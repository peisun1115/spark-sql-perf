#!/usr/bin/env bash

export SPARK_HOME="/tmp/spark"
export TPCDS_HOME="/tmp/tpcds"

#sudo yum -y install gcc
#sudo yum -y install bison
#sudo yum -y install byacc
#sudo yum -y install flex
#
#cd ${TPCDS_HOME}
#rm -rf kit; git clone https://github.com/peisun1115/tpcds-kit kit
#cd kit/tools
#cp Makefile.suite Makefile
#make 2>/dev/null

cd ${TPCDS_HOME}
wget https://s3-us-west-2.amazonaws.com/peis-autobot/tpcds/kit.tar.gz
tar xvfz kit.tar.gz

