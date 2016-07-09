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
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * Objectives: Test data frame performance
  * 1. Compare the following:
  * Source in EBS, Alluxio, S3,  Alluxio S3, Cache.
  * 2. Write performance.
  * Dst: EBS, Alluxio, S3, Alluxio with S3 as UFS.
  */

case class DataFrameConfig(
                            testName: String = "",
                            inputFile: String = "",
                            suffix: String = "",
                            storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                            size: Int = 500000000,
                            disabledTests: Seq[String]
                          ) {
  def inputFileName() = inputFile + suffix
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
    "free && sync && echo 3 > /proc/sys/vm/drop_caches && free" !!
  }

  def parquetWrite(spark: SparkContext, sqlContext: SQLContext,
                   config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    if (config.disabledTests.contains(config.testName)) return
    var start: Long = -1
    var end: Long = -1
    import sqlContext.implicits._

    var result = DataFrameResult(testName = config.testName)

    val df = spark.makeRDD(1 to config.size).map(i => (i, i * 2)).toDF("single", "double")
    df.select("single").where("single % 17112211 = 1").count()

    start = System.nanoTime()
    df.write.mode(saveMode = SaveMode.Overwrite).parquet(config.inputFileName())
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    dropBufferCache()

    printResult(result)
    results += result
  }

  def dfRead(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    if (config.disabledTests.contains(config.testName)) return
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
    printResult(result)
  }

  def dfPersist(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    if (config.disabledTests.contains(config.testName)) return
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
    result = result.copy(runTime2 = (end - start) / 1e9)

    df.unpersist()
    dropBufferCache()

    results += result
    printResult(result)
  }

  def printResult(result: DataFrameResult): Unit = {
    println(s"${result.testName}: [readTime ${result.readTime}] " +
      s"[cacheTime ${result.cacheTime}] [runTime1 ${result.runTime1}] [runTime2 ${result.runTime2}]")
  }

  def printResults(results: ArrayBuffer[DataFrameResult]): Unit = {
    for (result <- results) {
      printResult(result)
    }
  }

  // args(0): suffix
  // args(1): size
  // args(2): disabledTests
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrameBenchmark")
    val spark = new SparkContext(conf)

    val hadoopConf = spark.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))

    val sqlContext = new SQLContext(spark)

    val config = DataFrameConfig(suffix = args(0), size = args(1).toInt,
      disabledTests = args(2).split(",").toSeq)
    val results = ArrayBuffer.empty[DataFrameResult]

    parquetWrite(spark, sqlContext, config.copy(
      testName = "Write_EBS",
      inputFile = "/tmp/parquet"),
      results)
    parquetWrite(spark, sqlContext, config.copy(
      testName = "Write_S3",
      inputFile = "s3n://peis-autobot/alluxio_storage/parquet"),
      results)
    parquetWrite(spark, sqlContext, config.copy(
      testName = "Write_Alluxio",
      inputFile = "alluxio://localhost:19998/parquet_unused"),
      results)

    dfRead(sqlContext, config.copy(
      testName = "Read_EBS",
      inputFile = "/tmp/parquet"),
      results)
    dfRead(sqlContext, config.copy(
      testName = "Read_AlluxioOnS3",
      inputFile = "alluxio://localhost:19998/parquet"),
      results)
    dfRead(sqlContext, config.copy(
      testName = "Read_Alluxio",
      inputFile = "alluxio://localhost:19998/parquet"),
      results)
    dfRead(sqlContext, config.copy(
      testName = "Read_S3",
      inputFile = "s3n://peis-autobot/alluxio_storage/parquet"),
      results)

    dfPersist(sqlContext, config.copy(
      testName = "Read_Cache_Disk",
      inputFile = "/tmp/parquet",
      storageLevel = StorageLevel.DISK_ONLY),
      results)
    dfPersist(sqlContext, config.copy(
      testName = "Read_Cache_MemSer",
      inputFile = "/tmp/parquet",
      storageLevel = StorageLevel.MEMORY_ONLY_SER),
      results)
    dfPersist(sqlContext, config.copy(
      testName = "Read_Cache_Mem",
      inputFile = "/tmp/parquet",
      storageLevel = StorageLevel.MEMORY_ONLY),
      results)

    println("")
    printResults(results)
    spark.stop()
  }
}
