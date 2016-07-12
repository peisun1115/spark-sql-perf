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

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

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
    if (!config.enabledTests.contains(config.testNamePrefix) && !config.enabledTests.contains("ALL")) return
    var start: Long = -1
    var end: Long = -1
    import sqlContext.implicits._

    var result = DataFrameResult(testName = config.testName)

    val df = spark.makeRDD(1 to config.size).map(i => (i * 0.9, i, i * 1.2, i * 1.4, i * 1.8, i * 2.0)).toDF(
      "s1", "s2", "s3", "s4", "s5", "s6")

    start = System.nanoTime()
    df.write.mode(saveMode = SaveMode.Overwrite).parquet(config.inputFile)
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    dropBufferCache()

    printResult(config, result)
    results += result
  }

  def dfRead(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    if (!config.enabledTests.contains(config.testNamePrefix) && !config.enabledTests.contains("ALL")) return
    var start: Long = -1
    var end: Long = -1

    var result = DataFrameResult(testName = config.testName)

    start = System.nanoTime()
    val df = sqlContext.read.parquet(config.inputFile)
    end = System.nanoTime()
    result = result.copy(readTime = (end - start) / 1e9)

    start = System.nanoTime()
    df.agg(sum("s1"), sum("s2"), sum("s3"), sum("s4"), sum("s5"), sum("s6")).count()
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    start = System.nanoTime()
    df.agg(sum("s1"), sum("s2"), sum("s3"), sum("s4"), sum("s5"), sum("s6")).count()
    end = System.nanoTime()
    result = result.copy(runTime2 = (end - start) / 1e9)

    df.unpersist()
    dropBufferCache()

    results += result
    printResult(config, result)
  }

  def dfPersist(sqlContext: SQLContext, config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    if (!config.enabledTests.contains(config.testNamePrefix) && !config.enabledTests.contains("ALL")) return
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
    df.agg(sum("s1"), sum("s2"), sum("s3"), sum("s4"), sum("s5"), sum("s6")).count()
    end = System.nanoTime()
    result = result.copy(runTime1 = (end - start) / 1e9)

    start = System.nanoTime()
    df.agg(sum("s1"), sum("s2"), sum("s3"), sum("s4"), sum("s5"), sum("s6")).count()
    end = System.nanoTime()
    result = result.copy(runTime2 = (end - start) / 1e9)

    df.unpersist()
    dropBufferCache()

    results += result
    printResult(config, result)
  }

  def printResult(config: DataFrameConfig, result: DataFrameResult): Unit = {
    val printWriter = new PrintWriter(new BufferedWriter(new FileWriter(config.resultPath, true)))
    printWriter.println(s"${result.testName}: [readTime ${result.readTime}] " +
      s"[cacheTime ${result.cacheTime}] [runTime1 ${result.runTime1}] [runTime2 ${result.runTime2}]")
    printWriter.close()
  }

  def printResults(config: DataFrameConfig, results: ArrayBuffer[DataFrameResult]): Unit = {
    for (result <- results) {
      printResult(config, result)
    }
  }

  // args(0): input file name // This has to be the first name.
  // args(1): test name suffix
  // args(2): size
  // args(3): enabledTests separated by ",". "ALL" can be used to enable all.
  // args(4): output file
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrameBenchmark")
    val spark = new SparkContext(conf)

    val hadoopConf = spark.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))

    val sqlContext = new SQLContext(spark)

    val awsS3Bucket = sqlContext.getConf("spark.s3.awsS3Bcuekt", "peis-autobot");
    val alluxioMaster = sqlContext.getConf("spark.alluxio.master", "localhost:19998");

    val config = DataFrameConfig(
      suffix = args(1),
      size = args(2).toInt,
      enabledTests = args(3).split(",").toSet,
      resultPath = args(4))
    val results = ArrayBuffer.empty[DataFrameResult]

    parquetWrite(spark, sqlContext, config.copy(
      testNamePrefix = "Write_EBS",
      inputFile = "/tmp/parquet"),
      results)
    parquetWrite(spark, sqlContext, config.copy(
      testNamePrefix = "Write_S3",
      inputFile = s"s3n://${awsS3Bucket}/alluxio_storage_non_ufs/${args(0)}"),
      results)
    parquetWrite(spark, sqlContext, config.copy(
      testNamePrefix = "Write_Alluxio",
      inputFile = s"alluxio://${alluxioMaster}/${args(0)}_hot"),
      results)

    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_EBS",
      inputFile = "/tmp/parquet"),
      results)
    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_AlluxioOnS3",
      inputFile = s"alluxio://${alluxioMaster}/${args(0)}"),
      results)
    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_Alluxio",
      inputFile = s"alluxio://${alluxioMaster}/${args(0)}"),
      results)
    dfRead(sqlContext, config.copy(
      testNamePrefix = "Read_S3",
      inputFile = s"s3n://${awsS3Bucket}/alluxio_storage/${args(0)}"),
      results)

    dfPersist(sqlContext, config.copy(
      testNamePrefix = "Read_Cache_Disk",
      inputFile = s"/tmp/${args(0)}",
      storageLevel = StorageLevel.DISK_ONLY),
      results)
    dfPersist(sqlContext, config.copy(
      testNamePrefix = "Read_Cache_MemSer",
      inputFile = s"/tmp/${args(0)}",
      storageLevel = StorageLevel.MEMORY_ONLY_SER),
      results)
    dfPersist(sqlContext, config.copy(
      testNamePrefix = "Read_Cache_Mem",
      inputFile = s"/tmp/${args(0)}",
      storageLevel = StorageLevel.MEMORY_ONLY),
      results)
    spark.stop()
  }
}
