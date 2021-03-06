#!/usr/bin/env bash

SPARK_HOME=$1
TPCDS_HOME=$2
ALLUXIO_HOME=$3
SPARK_MASTER=$4
KIT_URI=$5
RESULT=$6

cd ${TPCDS_HOME}

if [[ -z ${KIT_URI} ]]; then
    KIT_URI=https://s3-us-west-2.amazonaws.com/peis-autobot/tpcds/kit.tar.gz
fi

INSTALL_KIT="rm -rf ${TPCDS_HOME}/kit*; mkdir -p ${TPCDS_HOME}/kit; cd ${TPCDS_HOME}; wget ${KIT_URI} -O kit.tar.gz; tar xvfz kit.tar.gz"
eval ${INSTALL_KIT}
for i in $(cat ${SPARK_HOME}/conf/slaves); do
    ssh -o "StrictHostKeyChecking=no" ${i} "${INSTALL_KIT}"
done

ALLUXIO_RESULT_DIR=$(cat ${SPARK_HOME}/conf/spark-defaults.conf | grep 'spark.sql.perf.results' | awk '{print $NF}')
${ALLUXIO_HOME}/bin/alluxio fs rm -R ${ALLUXIO_RESULT_DIR} || true
${SPARK_HOME}/bin/spark-submit --name "tpcds" --class "com.databricks.spark.sql.perf.RunTPCDS" --master ${SPARK_MASTER} ${TPCDS_HOME}/target/scala-2.11/spark-sql-perf-assembly-0.4.9-SNAPSHOT.jar

ALLUXIO_RESULT_FILENAME=$(${ALLUXIO_HOME}/bin/alluxio fs ls -R ${ALLUXIO_RESULT_DIR} | grep 'json' | awk 'END{print $NF}')
mkdir -p $(dirname ${TPCDS_HOME}/${RESULT})
${ALLUXIO_HOME}/bin/alluxio fs cat ${ALLUXIO_RESULT_FILENAME} > ${TPCDS_HOME}/${RESULT}
