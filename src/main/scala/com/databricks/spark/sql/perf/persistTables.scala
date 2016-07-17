package com.databricks.spark.sql.perf

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object persistTables {
  def main(args: Array[String]) {
    val alluxioPath = args(0)
    val hdfsPath = args(1)
    val tables = args(2).split(",").toSeq
    val conf = new SparkConf().setAppName("persistTables")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)

    for (table <- tables) {
      val df = sqlContext.read.parquet(s"${alluxioPath}/${table}")
      df.write.parquet(s"${hdfsPath}/${table}")
    }

    spark.stop()
  }
}
