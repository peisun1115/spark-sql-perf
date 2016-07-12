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

import java.io.{BufferedWriter, FileWriter, PrintWriter}

import org.apache.spark._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * Objectives:
  * 1. OFF_HEAP performance. Since Spark has removed this, it doesn't worth spending too much
  *    time on this.
  * 2. In RDD world, compare performance between Persist (ram_serialized, ram_deserialized, disk_serialized and
  *    SaveAsObjectFiles (alluxio, disk, and S3). Also compare results if we clear buffer cache.
  * 3. Figure out whether Persist still works when the input size increases.
  */

case class RunConfig(
                    testName: String = "",
                    inputFile: String = "",
                    saveAsFile: String = "",
                    suffix: String = System.nanoTime().toString,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    iterations: Int = 2,
                    enabledTests: Set[String],
                    resultPath: String = "/tmp/PersistBenchmark",
                    useTextFile: Boolean = false
                    ) {
  def saveAsFileName() = saveAsFile + "_" + suffix
}

case class Result(
                 testName: String = "",
                 saveTime: Double = -1,
                 runTime: ArrayBuffer[Double] = ArrayBuffer.empty[Double]
                 )

object PersistBenchmark {
  def dropBufferCache(): Unit = {
    "free && sync && echo 3 > /proc/sys/vm/drop_caches && free" !!
  }

  def saveAsBenchmark(spark: SparkContext, runConfig: RunConfig, results: ArrayBuffer[Result]): Unit = {
    if (!runConfig.enabledTests.contains(runConfig.testName) && !runConfig.enabledTests.contains("ALL")) return

    var a = spark.textFile(runConfig.inputFile)
    var start: Long = -1
    var end: Long = -1

    var result = Result(testName = runConfig.testName)

    // SaveAsObjectFile in local disk.
    start = System.nanoTime()
    if (runConfig.useTextFile) a.saveAsTextFile(runConfig.saveAsFileName) else a.saveAsObjectFile(runConfig.saveAsFileName)
    end = System.nanoTime()
    result = result.copy(saveTime = (end - start) / 1e9)
    if (runConfig.useTextFile) a = spark.textFile(runConfig.saveAsFileName) else a = spark.objectFile(runConfig.saveAsFileName)

    dropBufferCache

    for (i <- 1 to runConfig.iterations) {
      start = System.nanoTime()
      a.count()
      end = System.nanoTime()
      result.runTime += (end - start) / 1e9
    }

    a.unpersist()

    results += result
    printResult(runConfig, result)
    dropBufferCache
  }

  def persistBenchmark(spark: SparkContext, runConfig: RunConfig, results: ArrayBuffer[Result]): Unit = {
    if (!runConfig.enabledTests.contains(runConfig.testName) && !runConfig.enabledTests.contains("ALL")) return
    val a = spark.textFile(runConfig.inputFile)
    var start: Long = -1
    var end: Long = -1

    var result = Result(runConfig.testName)

    // SaveAs** in local disk
    start = System.nanoTime
    if (runConfig.storageLevel != StorageLevel.MEMORY_AND_DISK) {
      a.persist(runConfig.storageLevel)
    } else {
      a.cache()
    }
    end = System.nanoTime
    result = result.copy(saveTime = (end - start) / 1e9)

    dropBufferCache

    for (i <- 1 to runConfig.iterations) {
      start = System.nanoTime
      a.count()
      end = System.nanoTime
      result.runTime += (end - start) / 1e9
    }

    a.unpersist()

    results += result
    printResult(runConfig, result)
    dropBufferCache
  }

  def printResult(config: RunConfig, result: Result): Unit = {
    val printWriter = new PrintWriter(new BufferedWriter(new FileWriter(config.resultPath, true)))
    println(s"${result.testName}: [saveTime ${result.saveTime}] [runTime ${result.runTime}]")
  }

  def printResults(config: RunConfig, results: ArrayBuffer[Result]): Unit = {
    for (result <- results) {
      printResult(config, result)
    }
  }

  // args(0): inputFile
  // args(1): enabledTests separated by ",". "ALL" can be used to enable all.
  // args(2): output file
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PersistBenchmark")
    val spark = new SparkContext(conf)

    val hadoopConf = spark.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))

    val awsS3Bucket = spark.getConf.get("spark.s3.awsS3Bcuekt", "peis-autobot")
    val alluxioMaster = spark.getConf.get("spark.alluxio.master", "localhost:19998")

    val runConfig = RunConfig(
      inputFile = args(0),
      enabledTests = args(1).split(",").toSet[String],
      resultPath = args(2))
    val results = ArrayBuffer.empty[Result]

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Disk",
      saveAsFile = "/tmp/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Alluxio",
      saveAsFile = s"alluxio://${alluxioMaster}/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_S3",
      saveAsFile = s"s3n://${awsS3Bucket}/PersistBenchmark"), results)


    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsTextFile_Disk",
      useTextFile = true,
      saveAsFile = "/tmp/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsTextFile_Alluxio",
      useTextFile = true,
      saveAsFile = s"alluxio://${alluxioMaster}/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsTextFile_S3",
      useTextFile = true,
      saveAsFile = s"s3n://${awsS3Bucket}/PersistBenchmark"), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_MemoryOnly",
      storageLevel = StorageLevel.MEMORY_ONLY), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_MemoryOnlySer",
      storageLevel = StorageLevel.MEMORY_ONLY_SER), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_Cache",
      storageLevel = StorageLevel.MEMORY_AND_DISK), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_Disk",
      storageLevel = StorageLevel.DISK_ONLY), results)

    spark.stop()
  }
}