#!/usr/bin/env bash

export SPARK_HOME=${SPARK_HOME:-/tmp/spark}
export SPARK_BENCH_HOME=${SPARK_BENCH_HOME:-/tmp/spark_bench}
export ALLUXIO_UFS=${ALLUXIO_UFS:-s3://peis-autobot/alluxio_storage}

TEST_NAME=$1
shift
SPARK_MASTER=$1
shift

function prepareInput() {
    DIR=$(pwd)
    cd ~
    if [[ ! -a sample-1g ]]; then
        wget https://s3.amazonaws.com/alluxio-sample/datasets/sample-1g.gz
        gunzip sample-1g.gz
    fi
    FILENAME="sample"
    > ${FILENAME}
    for ((i = 0; i < ${1}; i++)); do
        cat sample-1g >> ${FILENAME}
    done
    cd ${DIR}
}

if [[ ${TEST_NAME} == "PrepareInput" ]]; then
    prepareInput "$@"
else
    ${SPARK_HOME}/bin/spark-submit --master ${SPARK_MASTER} --class "alluxio.benchmarks.${TEST_NAME}" \
        ${SPARK_BENCH_HOME}/target/scala-2.11/spark-sql-perf-assembly-0.4.9-SNAPSHOT.jar "$@"
    for arg in "$@"; do
      if [[ ${arg} =~ .*"Write_EBS".* ]]; then
            echo ${ALLUXIO_UFS}
            aws s3 rm --recursive ${ALLUXIO_UFS}/$1
            aws s3 cp --recursive /tmp/$1 ${ALLUXIO_UFS}/$1
        fi
    done
fi