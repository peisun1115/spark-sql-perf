/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.benchmarks

import java.util.Calendar

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.sys.process._

/**
  * Objectives:
  * 1.
  */

object DataFrameBenchmark {
  def dropBufferCache(): Unit = {
    "free && sync && echo 3 > /proc/sys/vm/drop_caches && free" !
  }

 def dfBenchmark(sqlContext: SQLContext, inputFile: String, cache: Boolean = false): Unit = {
    val start = Calendar.getInstance().getTimeInMillis()
    val df = sqlContext.read.parquet(inputFile)
    val readTime = Calendar.getInstance().getTimeInMillis()
    println(s"DataFrame benchmark $inputFile read.parquet took ${readTime - start}")

    df.select("single").where("single % 17112211 = 1").count()
    val first = Calendar.getInstance().getTimeInMillis()
    println(s"DataFrame benchmark $inputFile first query took ${first - readTime}")

    df.select("single").where("single % 17112211 = 1").count()
    val second = Calendar.getInstance().getTimeInMillis()
    println(s"DataFrame benchmark $inputFile second query took ${second - first}")

    if (cache) {
      df.cache()
      df.select("single", "double").where("single % 11112211 = 1").orderBy("single").join(df)
      df.select("single").where("single % 17112211 = 1").count()
      val queryTimeAfterCache0 = Calendar.getInstance().getTimeInMillis()
      println(s"DataFrame benchmark $inputFile third (cache required) query took ${queryTimeAfterCache0 - second}")

      df.select("single").where("single % 17112211 = 1").count()
      val queryTimeAfterCache1 = Calendar.getInstance().getTimeInMillis()
      println(s"DataFrame benchmark $inputFile forth (cache not required) query took ${queryTimeAfterCache1 - queryTimeAfterCache0}")
    }
  }

  def dfPersist(sqlContext: SQLContext, inputFile: String, level: StorageLevel): Unit = {
    val start = Calendar.getInstance().getTimeInMillis()
    val df = sqlContext.read.parquet(inputFile)
    val readTime = Calendar.getInstance().getTimeInMillis()
    println(s"DataFrame benchmark $inputFile read.parquet took ${readTime - start}")

    df.persist(level)

    df.select("single").where("single % 17112211 = 1").count()
    val first = Calendar.getInstance().getTimeInMillis()
    println(s"DataFrame benchmark $inputFile $level first query took ${first - readTime}")

    df.select("single").where("single % 17112211 = 1").count()
    val second = Calendar.getInstance().getTimeInMillis()
    println(s"DataFrame benchmark $inputFile $level second query took ${second - first}")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrameBenchmark")
    val spark = new SparkContext(conf)

    val hadoopConf = spark.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))


    val sqlContext = new SQLContext(spark)

    val now = Calendar.getInstance().getTime()
    println("Loading ", now)

    dfBenchmark(sqlContext, "/tmp/warmup")

    dfPersist(sqlContext, "alluxio://localhost:19998/parquet" + args(0), StorageLevel.DISK_ONLY)

    // Read parquet file from local disk, do a query, do another query.
    dfBenchmark(sqlContext, "/tmp/parquet" + args(0))

    // Read parquet file from alluxio, do a query, do another query.
    dfBenchmark(sqlContext, "alluxio://localhost:19998/parquet" + args(0))

    if (!args(1).isEmpty()) {
      // Read parquet file from S3, do a query, do another query.
      dfBenchmark(sqlContext, "s3n://peis-autobot/parquet" + args(0))
      dfPersist(sqlContext, "s3n://peis-autobot/parquet" + args(0), StorageLevel.DISK_ONLY)
      dfPersist(sqlContext, "s3n://peis-autobot/parquet" + args(0), StorageLevel.MEMORY_ONLY)
    }

    // dfPersist(sqlContext, "alluxio://localhost:19998/parquet" + args(0), StorageLevel.MEMORY_AND_DISK)
    dfPersist(sqlContext, "alluxio://localhost:19998/parquet" + args(0), StorageLevel.MEMORY_ONLY_SER)
    dfPersist(sqlContext, "alluxio://localhost:19998/parquet" + args(0), StorageLevel.MEMORY_ONLY)

    // Read parquet file, cache the data frame
    dfBenchmark(sqlContext, "/tmp/parquet" + args(0), true)

    spark.stop()
  }
}
