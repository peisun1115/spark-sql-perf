#!/usr/bin/env bash

export SPARK_HOME=${SPARK_HOME:-${1}}
export TPCDS_HOME=${TPCDS_HOME:-${2}}
export SPARK_MASTER=${SPARK_MASTER:-${3}}

RESULT=${4}

cd ${TPCDS_HOME}


INSTALL_KIT="rm -rf ${TPCDS_HOME}/kit*; mkdir -p ${TPCDS_HOME}/kit; cd ${TPCDS_HOME}; wget https://s3-us-west-2.amazonaws.com/peis-autobot/tpcds/kit.tar.gz; tar xvfz kit.tar.gz"
eval ${INSTALL_KIT}
for i in $(cat ${SPARK_HOME}/conf/slaves); do
    ssh -o "StrictHostKeyChecking=no" ${i} "${INSTALL_KIT}"
done

RESULT_PATH_S3=$(cat ${SPARK_HOME}/conf/spark-defaults.conf | grep 'spark.sql.perf.results' | awk '{print $NF}'| sed 's/s3n/s3/' | sed 's/s3a/s3/')
echo "${RESULT_PATH_S3}"
aws s3 --recursive rm  ${RESULT_PATH_S3}
${SPARK_HOME}/bin/spark-submit --name "tpcds" --class "com.databricks.spark.sql.perf.RunTPCDS" --master ${SPARK_MASTER} ${TPCDS_HOME}/target/scala-2.11/spark-sql-perf-assembly-0.4.9-SNAPSHOT.jar

# s3://peis-autobot/tpcds/results
S3=$(echo ${RESULT_PATH_S3} | cut -d '/' -f1)
BUCKET=$(echo ${RESULT_PATH_S3} | cut -d '/' -f3)

RESULT_FILE_S3=$(aws s3 ls --recursive ${RESULT_PATH_S3} | grep 'json' | awk 'END{print $NF}')
RESULT_FILE_S3="${S3}//${BUCKET}/${RESULT_FILE_S3}"
aws s3 cp ${RESULT_FILE_S3} ${TPCDS_HOME}/${RESULT}
