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

import java.io.{BufferedWriter, ByteArrayOutputStream, FileWriter, PrintWriter}

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
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
                    testNamePrefix: String = "",
                    testNameSuffix: String = "",
                    inputFile: String = "",
                    saveAsFile: String = "",
                    suffix: String = System.nanoTime().toString,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    iterations: Int = 2,
                    enabledTests: Set[String],
                    resultPath: String = "/tmp/PersistBenchmark",
                    useTextFile: Boolean = false,
                    useKyro: Boolean = false
                    ) {
  def testName() = testNamePrefix + testNameSuffix
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
    if (!runConfig.enabledTests.contains(runConfig.testNamePrefix) && !runConfig.enabledTests.contains("ALL")) return

    var a = spark.textFile(runConfig.inputFile)
    var start: Long = -1
    var end: Long = -1

    var result = Result(testName = runConfig.testName)

    // SaveAsObjectFile in local disk.
    start = System.nanoTime()
    if (runConfig.useTextFile) {
      a.saveAsTextFile(runConfig.saveAsFileName)
    } else if (runConfig.useKyro) {
      saveAsObjectFile(a, runConfig.saveAsFileName)
    } else {
      a.saveAsObjectFile(runConfig.saveAsFileName)
    }
    end = System.nanoTime()
    result = result.copy(saveTime = (end - start) / 1e9)
    if (runConfig.useTextFile) {
      a = spark.textFile(runConfig.saveAsFileName)
    } else if (runConfig.useKyro) {
      a = objectFile(spark, runConfig.saveAsFileName)
    } else {
      a = spark.objectFile(runConfig.saveAsFileName)
    }

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
    if (!runConfig.enabledTests.contains(runConfig.testNamePrefix) && !runConfig.enabledTests.contains("ALL")) return
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
    printWriter.println(s"${result.testName}: [saveTime ${result.saveTime}] [runTime ${result.runTime}]")
    printWriter.close()
  }

  def printResults(config: RunConfig, results: ArrayBuffer[Result]): Unit = {
    for (result <- results) {
      printResult(config, result)
    }
  }

  // copied from https://github.com/phatak-dev/blog/blob/master/code/kryoexample/src/main/scala/com/madhu/spark/kryo/KryoExample.scala

  /*
 * Used to write as Object file using kryo serialization
 */
  def saveAsObjectFile[T: ClassTag](rdd: RDD[T], path: String) {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => iter.grouped(10)
      .map(_.toArray))
      .map(splitArray => {
        //initializes kyro and calls your registrator class
        val kryo = kryoSerializer.newKryo()

        //convert data to bytes
        val bao = new ByteArrayOutputStream()
        val output = kryoSerializer.newKryoOutput()
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, splitArray)
        output.close()

        // We are ignoring key field of sequence file
        val byteWritable = new BytesWritable(bao.toByteArray)
        (NullWritable.get(), byteWritable)
      }).saveAsSequenceFile(path)
  }

  /*
   * Method to read from object file which is saved kryo format.
   */
  def objectFile[T](sc: SparkContext, path: String, minPartitions: Int = 1)(implicit ct: ClassTag[T]) = {
    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => {
        val kryo = kryoSerializer.newKryo()
        val input = new Input()
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })
  }

  // args(0): TestNameSuffix
  // args(1): inputFile
  // args(2): enabledTests separated by ",". "ALL" can be used to enable all.
  // args(3): output file
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PersistBenchmark")
    val spark = new SparkContext(conf)

    val hadoopConf = spark.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))

    val awsS3Bucket = spark.getConf.get("spark.s3.awsS3Bcuekt", "peis-autobot")
    val alluxioMaster = spark.getConf.get("spark.alluxio.master", "localhost:19998")

    val runConfig = RunConfig(
      testNameSuffix = args(0),
      inputFile = args(1),
      enabledTests = args(2).split(",").toSet[String],
      resultPath = args(3))
    val results = ArrayBuffer.empty[Result]

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsObjectFile_Disk",
      saveAsFile = "/tmp/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsObjectFile_Alluxio",
      saveAsFile = s"alluxio://${alluxioMaster}/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsObjectFile_S3",
      saveAsFile = s"s3n://${awsS3Bucket}/PersistBenchmark"), results)


    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsKyroFile_Disk",
      useKyro = true,
      saveAsFile = "/tmp/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsKyroFile_Alluxio",
      useKyro = true,
      saveAsFile = s"alluxio://${alluxioMaster}/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsKyroFile_S3",
      useKyro = true,
      saveAsFile = s"s3n://${awsS3Bucket}/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsTextFile_Disk",
      useTextFile = true,
      saveAsFile = "/tmp/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsTextFile_Alluxio",
      useTextFile = true,
      saveAsFile = s"alluxio://${alluxioMaster}/PersistBenchmark"), results)

    saveAsBenchmark(spark, runConfig.copy(
      testNamePrefix = "SaveAsTextFile_S3",
      useTextFile = true,
      saveAsFile = s"s3n://${awsS3Bucket}/PersistBenchmark"), results)

    persistBenchmark(spark, runConfig.copy(
      testNamePrefix = "Persist_MemoryOnly",
      storageLevel = StorageLevel.MEMORY_ONLY), results)

    persistBenchmark(spark, runConfig.copy(
      testNamePrefix = "Persist_MemoryOnlySer",
      storageLevel = StorageLevel.MEMORY_ONLY_SER), results)

    persistBenchmark(spark, runConfig.copy(
      testNamePrefix = "Persist_Cache",
      storageLevel = StorageLevel.MEMORY_AND_DISK), results)

    persistBenchmark(spark, runConfig.copy(
      testNamePrefix = "Persist_Disk",
      storageLevel = StorageLevel.DISK_ONLY), results)

    spark.stop()
  }
}