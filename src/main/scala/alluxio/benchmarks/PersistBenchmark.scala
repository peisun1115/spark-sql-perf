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
                    suffix: String = System.nanoTime().toString,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    iterations: Int = 3,
                    dropBufferCache: Boolean = false
                    ) {
  def saveAsFileName() = saveAsFile + "_" + suffix
}

case class Result(
                 testName: String = "",
                 saveTime: Double = -1,
                 runTime: Double = -1
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
    start = System.nanoTime()
    b.saveAsObjectFile(runConfig.saveAsFileName)
    end = System.nanoTime()
    result = result.copy(saveTime = (end - start) / 1e9)

    b = spark.objectFile(runConfig.saveAsFileName)

    if (runConfig.dropBufferCache) dropBufferCache

    start = System.nanoTime()
    for (i <- 1 to runConfig.iterations) {
      b.reduceByKey(_ + _).count()
    }
    end = System.nanoTime()
    result = result.copy(runTime = ((end - start) / 1e9) / 1e9)

    b.unpersist()

    results += result
    dropBufferCache
  }

  def persistBenchmark(spark: SparkContext, runConfig: RunConfig, results: ArrayBuffer[Result]): Unit = {
    val a = spark.textFile(runConfig.inputFile)
    var start: Long = -1
    var end: Long = -1

    var result = Result(runConfig.testName)

    // SaveAs** in local disk
    val b = a.flatMap(x => x.split(" ")).map(x => (x, 1))
    start = System.nanoTime
    b.persist(runConfig.storageLevel)
    end = System.nanoTime
    result = result.copy(saveTime = (end - start) / 1e9)

    if (runConfig.dropBufferCache) dropBufferCache

    start = System.nanoTime
    for (i <- 1 to runConfig.iterations) {
      b.reduceByKey(_ + _).count()
    }
    end = System.nanoTime
    result = result.copy(runTime = (end - start) / 1e9)

    b.unpersist()

    results += result
    dropBufferCache
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
      saveAsFile = "/tmp/PersistBenchmark1"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Disk_BufferCacheOff",
      saveAsFile = "/tmp/PersistBenchmark2", dropBufferCache = true), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Alluxio_BufferCacheOn",
      saveAsFile = "alluxio://localhost:19998/PersistBenchmark1"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testName = "SaveAsObjectFile_Alluxio_BufferCacheOff",
      saveAsFile = "alluxio://localhost:19998/PersistBenchmark2", dropBufferCache = true), results)

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