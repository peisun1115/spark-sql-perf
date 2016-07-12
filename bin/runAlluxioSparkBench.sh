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
    FILENAME="sample-${i}g"
    if [[ ! -a ${FILENAME} ]]; then
        touch ${FILENAME}
        for ((i = 0; i < ${1}; i++)); do
            cat sample-1g >> ${FILENAME}
        done
    fi
    cd ${DIR}
}

if [[ ${TEST_NAME} == "PrepareInput" ]]; then
    prepareInput "$@"
else
    ${SPARK_HOME}/bin/spark-submit --master ${SPARK_MASTER} --class "alluxio.benchmarks.${TEST_NAME}" \
        ${SPARK_BENCH_HOME}/target/scala-2.11/spark-sql-perf-assembly-0.4.9-SNAPSHOT.jar "$@"
    if [[ ${TEST_NAME} == "Write_EBS" ]]; then
        aws s3 rm --recursive ${ALLUXIO_UFS}/parquet
        aws s3 cp --recursive /tmp/parquet ${ALLUXIO_UFS}/parquet
     fi
fi