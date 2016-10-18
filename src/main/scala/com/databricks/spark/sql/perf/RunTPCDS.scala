/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


case class RunTPCDSConfig(
    generateInput: Boolean = true,
    dsdgenDir: String = "",
    scaleFactor: Int = 1,
    inputDir: String = "",
    tableFilter: String = "",
    databaseName: String = "",
    format: String = "parquet",
    partitionTables: Boolean = false,
    clusterByPartitionColumns: Boolean = false,
    filter: Option[String] = None,
    overwrite: Boolean = false,
    iterations: Int = 3,
    partitions: Int = 20)

/**
  * Runs a benchmark and prints the results to the screen.
  * Configs:
  * spark.sql.perf.results
  *
  * spark.sql.perf.generate.input
  * spark.sql.perf.dsdgen.dir
  * spark.sql.perf.scale.factor
  * spark.sql.perf.input.dir
  * spark.sql.perf.table.filter
  *
  * spark.sql.database.name
  * spark.sql.perf.format
  * spark.sql.perf.partition.tables
  * spark.sql.perf.cluster.partition.columns
  *
  * spark.sql.perf.filter
  * spark.sql.perf.iterations
  */
object RunTPCDS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)


    val sc = SparkContext.getOrCreate(conf)

    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3.awsAccessKeyId", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    hadoopConf.set("fs.s3.awsSecretAccessKey", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val generateInput = sqlContext.getConf("spark.sql.perf.generate.input", "false").toBoolean
    val dsdgenDir = sqlContext.getConf("spark.sql.perf.dsdgen.dir")
    val scaleFactor = sqlContext.getConf("spark.sql.perf.scale.factor", "1").toInt
    val inputDir = sqlContext.getConf("spark.sql.perf.input.dir")
    val filter = sqlContext.getConf("spark.sql.perf.filter", "")
    val iterations = sqlContext.getConf("spark.sql.perf.iterations", "3").toInt
    val tableFilter = sqlContext.getConf("spark.sql.perf.table.filter", "")
    val partionTables = sqlContext.getConf("spark.sql.perf.partition.tables", "false").toBoolean
    val clusterByPartitionColumns = sqlContext.getConf("spark.sql.perf.cluster.partition.columns", "false").toBoolean
    val overwrite = sqlContext.getConf("spark.sql.perf.overwrite", "false").toBoolean
    val partitions = sqlContext.getConf("spark.sql.perf.partitions", "20").toInt
    val databaseName = sqlContext.getConf("spark.sql.perf.database", "")

    val config = RunTPCDSConfig(
      generateInput = generateInput,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      tableFilter = tableFilter,
      inputDir = inputDir,
      partitionTables = partionTables,
      clusterByPartitionColumns = clusterByPartitionColumns,
      filter = Some(filter),
      overwrite = overwrite,
      iterations = iterations,
      partitions = partitions,
      databaseName = databaseName)

    createTable(sqlContext, config)
    run(sqlContext, config)
    sc.stop()
  }

  def createTable(sqlContext: SQLContext, config: RunTPCDSConfig): Unit = {
    val conf = sqlContext.getAllConfs
    val tables = new Tables(sqlContext, config.dsdgenDir, config.scaleFactor)
    if (config.generateInput) {
      tables.genData(
        config.inputDir, config.format, config.overwrite, config.partitionTables, true,
        clusterByPartitionColumns = config.clusterByPartitionColumns, true,
        config.tableFilter, config.partitions)
    }
    if (!config.databaseName.isEmpty) {
      tables.createExternalTables(config.inputDir, config.format, config.databaseName, true)
    } else {
      tables.createTemporaryTables(config.inputDir, config.format)
    }
  }

  def run(sqlContext: SQLContext, config: RunTPCDSConfig): Unit = {
    import sqlContext.implicits._

    val benchmark = new TPCDS(sqlContext = sqlContext)

    val allQueries = config.filter.map { f =>
      benchmark.all.filter(_.name matches f)
    } getOrElse {
      benchmark.all
    }

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = config.iterations)

    experiment.waitForFinish(1000 * 60 * 30)
  }
}
