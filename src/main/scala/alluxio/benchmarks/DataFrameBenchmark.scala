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

import java.io.{File, PrintWriter}

import org.apache.spark._
import org.apache.spark.sql.functions._
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
                            testNamePrefix: String = "",
                            inputFile: String = "",
                            suffix: String = "",
                            storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                            size: Int = 500000000,
                            enabledTests: Set[String],
                            resultPath: String = "/tmp/result"
                          ) {
  def testName() = testNamePrefix + suffix
}

case class DataFrameResult(
                            testName: String = "",
                            testSize: String = "",
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
    if (!config.enabledTests.contains(config.testName) && !config.enabledTests.contains("ALL")) return
    var start: Long = -1
    var end: Long = -1
    import sqlContext.implicits._

    var result = DataFrameResult(testName = config.testName)

    val df = spark.makeRDD(1 to config.size).map(i => (i, i * 2)).toDF("single", "double")

    start = System.nanoTime()
    df.write.mode(saveMode = SaveMode.Overwrite).parquet(config.inputFile)
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    dropBufferCache()

    printResult(config, result)
    results += result
  }

  def dfRead(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    if (!config.enabledTests.contains(config.testName) && !config.enabledTests.contains("ALL")) return
    var start: Long = -1
    var end: Long = -1

    var result = DataFrameResult(testName = config.testName)

    start = System.nanoTime()
    val df = sqlContext.read.parquet(config.inputFile)
    end = System.nanoTime()
    result = result.copy(readTime = (end - start) / 1e9)

    start = System.nanoTime()
    df.agg(sum("single"), sum("double")).count()
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    start = System.nanoTime()
    df.agg(sum("single"), sum("double")).count()
    end = System.nanoTime()
    result = result.copy(runTime2 = (end - start) / 1e9)

    df.unpersist()
    dropBufferCache()

    results += result
    printResult(config, result)
  }

  def dfPersist(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    if (!config.enabledTests.contains(config.testName) && !config.enabledTests.contains("ALL")) return
    var start: Long = -1
    var end: Long = -1

    var result = DataFrameResult(testName = config.testName)

    start = System.nanoTime()
    val df = sqlContext.read.parquet(config.inputFile)
    end = System.nanoTime()
    result = result.copy(readTime = (end - start) / 1e9)

    start = System.nanoTime()
    if (config.storageLevel == StorageLevel.MEMORY_ONLY) {
      df.cache()
    } else {
      df.persist(config.storageLevel)
    }

    end = System.nanoTime()
    result = result.copy(cacheTime = (end - start) / 1e9)

    start = System.nanoTime()
    df.agg(sum("single"), sum("double")).count()
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    start = System.nanoTime()
    df.agg(sum("single"), sum("double")).count()
    end = System.nanoTime()
    result = result.copy(runTime2 = (end - start) / 1e9)

    df.unpersist()
    dropBufferCache()

    results += result
    printResult(config, result)
  }

  def printResult(config: DataFrameConfig, result: DataFrameResult): Unit = {
    val printWriter = new PrintWriter(new File(config.resultPath))
    printWriter.println(s"${result.testName}: [readTime ${result.readTime}] " +
      s"[cacheTime ${result.cacheTime}] [runTime1 ${result.runTime1}] [runTime2 ${result.runTime2}]")
    printWriter.close()
  }

  def printResults(config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    for (result <- results) {
      printResult(config, result)
    }
  }

  // args(0): test name suffix.
  // args(1): size
  // args(2): enabledTests separated by ",". "ALL" can be used to enable all.
  // args(3): output file
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrameBenchmark")
    val spark = new SparkContext(conf)

    val hadoopConf = spark.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))

    val sqlContext = new SQLContext(spark)

    val awsS3Bucket = sqlContext.getConf("spark.s3.awsS3Bcuekt", "peis-autobot");
    val alluxioMaster = sqlContext.getConf("spark.alluxio.master", "localhost:19998");

    val config = DataFrameConfig(suffix = args(0), size = args(1).toInt,
      enabledTests = args(2).split(",").toSet,
      resultPath = args(3))
    val results = ArrayBuffer.empty[DataFrameResult]

    parquetWrite(spark, sqlContext, config.copy(
      testNamePrefix = "Write_EBS",
      inputFile = "/tmp/parquet"),
      results)
    parquetWrite(spark, sqlContext, config.copy(
      testNamePrefix = "Write_S3",
      inputFile = s"s3n://${awsS3Bucket}/alluxio_storage_non_ufs/parquet"),
      results)
    parquetWrite(spark, sqlContext, config.copy(
      testNamePrefix = "Write_Alluxio",
      inputFile = s"alluxio://${alluxioMaster}/parquet_hot"),
      results)

    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_EBS",
      inputFile = "/tmp/parquet"),
      results)
    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_AlluxioOnS3",
      inputFile = s"alluxio://${alluxioMaster}/parquet"),
      results)
    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_Alluxio",
      inputFile = s"alluxio://${alluxioMaster}/parquet_hot"),
      results)
    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_S3",
      inputFile = s"s3n://${awsS3Bucket}/alluxio_storage_non_ufs/parquet"),
      results)

    dfPersist(sqlContext, config.copy(
      testNamePrefix = "Read_Cache_Disk",
      inputFile = "/tmp/parquet",
      storageLevel = StorageLevel.DISK_ONLY),
      results)
    dfPersist(sqlContext, config.copy(
      testNamePrefix = "Read_Cache_MemSer",
      inputFile = "/tmp/parquet",
      storageLevel = StorageLevel.MEMORY_ONLY_SER),
      results)
    dfPersist(sqlContext, config.copy(
      testNamePrefix = "Read_Cache_Mem",
      inputFile = "/tmp/parquet",
      storageLevel = StorageLevel.MEMORY_ONLY),
      results)
    spark.stop()
  }
}
