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
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * Objectives:
  * 1. OFF_HEAP performance. Done. No big regression. Since Spark has removed this, it doesn't worth spending too much
  *    time on this.
  * 2. In RDD world, compare performance between Persist (ram_serialized, ram_deserialized, disk_serialized and
  *    SaveAsObjectFiles (alluxio, disk, and S3). Also compare results if we clear buffer cache.
  * 3. Figure out whether Persist still works when the input size increases.
  */

case class RunConfig(
                    testName: String = "",
                    inputFile: String = "",
                    saveAsFile: String = "",
                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    iterations: Int = 3,
                    dropBufferCache: Boolean = false
                    )
case class Result(
                 testName: String = "",
                 saveTime: Long = -1,
                 runTime: Long = -1
                 )

object PersistBenchmark {
  def dropBufferCache(): Unit = {
    "free && sync && echo 3 > /proc/sys/vm/drop_caches && free" !
  }

  def saveAsBenchmark(spark: SparkContext, runConfig: RunConfig, results: ArrayBuffer[Result]): Unit = {
    val a = spark.textFile(runConfig.inputFile)
    var start: Long = -1
    var end: Long = -1

    var result = Result(testName = runConfig.testName)

    // SaveAsObjectFile in local disk.
    var b = a.flatMap(x => x.split(" ")).map(x => (x, 1))
    start = Calendar.getInstance.getTimeInMillis
    b.saveAsObjectFile(runConfig.saveAsFile)
    end = Calendar.getInstance.getTimeInMillis
    result = result.copy(saveTime = end - start)

    b = spark.objectFile(runConfig.saveAsFile)

    if (runConfig.dropBufferCache) dropBufferCache

    start = Calendar.getInstance().getTimeInMillis
    for (i <- 1 to runConfig.iterations) {
      b.reduceByKey(_ + _)
    }
    end = Calendar.getInstance().getTimeInMillis
    result = result.copy(runTime = end - start)

    b.unpersist()

    results += result
  }

  def persistBenchmark(spark: SparkContext, runConfig: RunConfig, results: ArrayBuffer[Result]): Unit = {
    val a = spark.textFile(runConfig.inputFile)
    var start: Long = -1
    var end: Long = -1

    var result = Result(runConfig.testName)

    // SaveAs** in local disk
    val b = a.flatMap(x => x.split(" ")).map(x => (x, 1))
    start = Calendar.getInstance.getTimeInMillis
    b.persist(runConfig.storageLevel)
    end = Calendar.getInstance.getTimeInMillis
    result = result.copy(saveTime = end - start)

    if (runConfig.dropBufferCache) dropBufferCache

    start = Calendar.getInstance.getTimeInMillis
    for (i <- 1 to runConfig.iterations) {
      b.reduceByKey(_ + _)
    }
    end = Calendar.getInstance.getTimeInMillis
    result = result.copy(runTime = end - start)

    b.unpersist()

    results += result
  }

  def printResults(results: ArrayBuffer[Result]): Unit = {
    for (result <- results) {
      println(s"${result.testName}: [saveTime ${result.saveTime}] [runTime ${result.runTime}]")
    }
  }

  // args(0): inputfile
  // args(1): iterations
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PersistBenchmark")
    val spark = new SparkContext(conf)

    val runConfig = RunConfig(inputFile = args(0), iterations = args(1).toInt)
    val results = ArrayBuffer.empty[Result]

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Disk_BufferCacheOn",
      saveAsFile = "/tmp/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Disk_BufferCacheOff",
      saveAsFile = "/tmp/PersistBenchmark", dropBufferCache = true), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Alluxio_BufferCacheOn",
      saveAsFile = "alluxio://localhost:19998/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Alluxio_BufferCacheOff",
      saveAsFile = "alluxio://localhost:19998/PersistBenchmark", dropBufferCache = true), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_MemoryOnly_BufferCacheOn",
      storageLevel = StorageLevel.MEMORY_ONLY), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_MemoryOnlySer_BufferCacheOn",
      storageLevel = StorageLevel.MEMORY_ONLY_SER), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_Disk_BufferCacheOn",
      storageLevel = StorageLevel.DISK_ONLY), results)

    persistBenchmark(spark, runConfig.copy(
      testName = "Persist_Disk_BufferCacheOff",
      storageLevel = StorageLevel.DISK_ONLY,
      dropBufferCache = true), results)

    printResults(results)

    spark.stop()
  }
}