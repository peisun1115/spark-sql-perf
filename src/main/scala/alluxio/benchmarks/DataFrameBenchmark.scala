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

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * Objectives: Test data frame performance
  * 1. Compare the following:
  * Source in EBS, Alluxio, S3,  Alluxio S3.
  * 2. Write performance.
  * Dst: EBS, Alluxio, S3, Alluxio with S3 as UFS.
  */

case class DataFrameConfig(
                            testName: String = "",
                            inputFile: String = "",
                            suffix: String = "",
                            storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                          ) {
  def inputFileName() = inputFile + "_" + suffix
}

case class DataFrameResult(
                            testName: String = "",
                            readTime: Double = -1,
                            cacheTime: Double = -1,
                            runTime1: Double = -1,
                            runTime2: Double = -1
                          )

object DataFrameBenchmark {
  def dropBufferCache(): Unit = {
    "free && sync && echo 3 > /proc/sys/vm/drop_caches && free" !
  }

  def dfRead(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    var start: Long = -1
    var end: Long = -1

    var result = DataFrameResult(testName = config.testName)

    start = System.nanoTime()
    val df = sqlContext.read.parquet(config.inputFileName())
    end = System.nanoTime()
    result = result.copy(readTime = (end - start) / 1e9)

    start = System.nanoTime()
    df.select("single").where("single % 17112211 = 1").count()
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    start = System.nanoTime()
    df.select("single").where("single % 17112211 = 1").count()
    end = System.nanoTime()
    result = result.copy(runTime2 = (end - start) / 1e9)

    df.unpersist()
    dropBufferCache()
    results += result
  }

  def dfPersist(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    var start: Long = -1
    var end: Long = -1

    var result = DataFrameResult(testName = config.testName)

    start = System.nanoTime()
    val df = sqlContext.read.parquet(config.inputFileName())
    end = System.nanoTime()
    result = result.copy(readTime = (end - start) / 1e9)

    start = System.nanoTime()
    df.persist(config.storageLevel)
    end = System.nanoTime()
    result = result.copy(cacheTime = (end - start) / 1e9)

    start = System.nanoTime()
    df.select("single").where("single % 17112211 = 1").count()
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    start = System.nanoTime()
    df.select("single").where("single % 17112211 = 1").count()
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    df.unpersist()
    dropBufferCache()
    results += result
  }

  def printResult(result: DataFrameResult): Unit = {
    println(s"${result.testName}: [readTime ${result.readTime}] " +
      s"[cacheTime ${result.cacheTime}] [runTime1 ${result.runTime1}] [runTime2 ${result.runTime2}]")
  }

  def printResults(results: ArrayBuffer[DataFrameResult): Unit = {
    for (result <- results) {
      printResult(result)
    }
  }

  // args(0): suffix
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrameBenchmark")
    val spark = new SparkContext(conf)

    val hadoopConf = spark.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))

    val sqlContext = new SQLContext(spark)

    val runConfig = DataFrameConfig(suffix = args(0))
    val results = ArrayBuffer.empty[DataFrameResult]
/*
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
*/
    spark.stop()
  }
}
