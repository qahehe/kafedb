/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql.dex
// scalastyle:off

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
// For datagens
import java.io._
import scala.sys.process._


object TPCHDataGen {

  val benchmark = "TPCH"
  //val scaleFactors = Seq("1", "10", "100", "1000", "10000") // "1", "10", "100", "1000", "10000" list of scale factors to generate and import
  val scaleFactors = Seq("1") // "1", "10", "100", "1000", "10000" list of scale factors to generate and import
  val baseLocation = s"bench/data"
  val baseDatagenFolder = "bench/datagen"

  // Output file formats
  val fileFormat = "parquet" // only parquet was tested
  val shuffle = true // If true, partitions will be coalesced into a single file during generation up to spark.sql.files.maxRecordsPerFile (if set)
  val overwrite = false //if to delete existing files (doesn't check if results are complete on no-overwrite)

  // Generate stats for CBO
  val createTableStats = false
  val createColumnStats = false

  val dbSuffix = "postgres"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TPCH Data Generation")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //val workers: Int = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt //number of nodes, assumes one executor per node
    //val workers: Int = spark.sparkContext.getConf.getInt("spark.executor.instances", 1)
    val workers: Int = 1
    val cores: Int = Runtime.getRuntime.availableProcessors //number of CPU-cores

    // Set Spark config to produce same and comparable source files across systems
    // do not change unless you want to derive from default source file composition, in that case also set a DB suffix
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    // Prevent very large files. 20 million records creates between 500 and 1500MB files in TPCH
    spark.sqlContext.setConf("spark.sql.files.maxRecordsPerFile", "20000000")  // This also regulates the file coalesce

    waitForWorkers(spark, workers, 3600) //wait up to an hour

    // install (build) the data generators in all nodes
    val worker = 1l
    s"TPCH worker $worker\n" + installDBGEN(baseFolder = baseDatagenFolder)(worker)

    // Generate the data, import the tables, generate stats for selected benchmarks and scale factors
    scaleFactors.foreach { scaleFactor =>
      // First set some config settings affecting OOMs/performance
      setScaleConfig(spark, scaleFactor)

      val (dbname, tables, location) = getBenchmarkData(spark, scaleFactor)

      // Generate data
      time {
        println(s"Generating data for $benchmark SF $scaleFactor at $location")
        generateData(tables, location, scaleFactor, workers, cores)
      }

      // Import data to Spark
      time {
        println(s"\nImporting data for $benchmark into DB $dbname from $location")
        importData(spark, dbname, tables, location)
      }
      if (createTableStats) time {
        println(s"\nGenerating table statistics for DB $dbname (with analyzeColumns=$createColumnStats)")
        tables.analyzeTables(dbname, analyzeColumns = createColumnStats)
      }

      // Validate data in Spark
      //validate(spark, scaleFactor, dbname)
    }
  }

  private def validate(spark: SparkSession, scaleFactor: String, dbname: String): Unit = {
    // Validate data on Spark
    spark.sql(s"use $dbname")
    time {
      spark.sql(s"show tables").select("tableName").collect().foreach { tableName =>
        val name: String = tableName.toString().drop(1).dropRight(1)
        println(s"Printing table information for $benchmark SF $scaleFactor table $name")
        val count = spark.sql(s"select count(*) as ${name}_count from $name").collect()(0)(0)
        println(s"Table $name has " + util.Try(spark.sql(s"SHOW PARTITIONS $name").count() + " partitions")
          .getOrElse(s"no partitions") + s" and $count rows.")
        spark.sql(s"describe extended $name").show(999, truncate = false)
      }
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis() //nanoTime()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis() //nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  def installDBGEN(url: String = "https://github.com/databricks/tpch-dbgen.git",
                   useStdout: Boolean = true,
                   baseFolder: String = "/tmp")(i: java.lang.Long): String = {
    // check if we want the revision which makes dbgen output to stdout
    val checkoutRevision: String = if (useStdout) "git checkout 0469309147b42abac8857fa61b4cf69a6d3128a8 -- bm_utils.c" else ""
    Seq("mkdir", "-p", baseFolder).!
    val pw = new PrintWriter(new File(s"$baseFolder/dbgen_$i.sh" ))
    pw.write(
      s"""
         |rm -rf $baseFolder/dbgen
         |rm -rf $baseFolder/dbgen_install_$i
         |mkdir $baseFolder/dbgen_install_$i
         |cd $baseFolder/dbgen_install_$i
         |git clone '$url'
         |cd tpch-dbgen
         |$checkoutRevision
         |make
         |ln -sf $baseFolder/dbgen_install_$i/tpch-dbgen $baseFolder/dbgen || echo "ln -sf failed"
         |test -e $baseFolder/dbgen/dbgen
         |echo "OK"
       """.stripMargin)
    pw.close()
    Seq("chmod", "+x", s"$baseFolder/dbgen_$i.sh").!
    Seq(s"$baseFolder/dbgen_$i.sh").!!
  }

  // Checks that we have the correct number of worker nodes to start the data generation
  // Make sure you have set the workers variable correctly, as the datagens binaries need to be present in all nodes
  def waitForWorkers(spark: SparkSession, requiredWorkers: Int, tries: Int) : Unit = {
    def numWorkers: Int = spark.sparkContext.getExecutorMemoryStatus.size - 1
    for (i <- 0 until tries) {
      if (numWorkers == requiredWorkers) {
        println(s"Waited ${i}s. for $numWorkers workers to be ready")
        return
      }
      if (i % 60 == 0) println(s"Waiting ${i}s. for workers to be ready, got only $numWorkers workers")
      Thread sleep 1000
    }
    throw new Exception(s"Timed out waiting for workers to be ready after ${tries}s.")
  }

  def getBenchmarkData(spark: SparkSession, scaleFactor: String): (String, TPCHTables, String) =
    (
      s"tpch_sf${scaleFactor}_$fileFormat$dbSuffix",
      new TPCHTables(
        spark.sqlContext,
        dbgenDir = s"$baseDatagenFolder/dbgen",
        scaleFactor,
        useDoubleForDecimal = false,
        useStringForDate = false,
        generatorParams = Nil
      ),
      s"$baseLocation/tpch/sf${scaleFactor}_$fileFormat"
    )

  def setScaleConfig(spark: SparkSession, scaleFactor: String): Unit = {
    // Avoid OOM when shuffling large scale fators
    // and errors like 2GB shuffle limit at 10TB like: Most recent failure reason: org.apache.spark.shuffle.FetchFailedException: Too large frame: 9640891355
    // For 10TB 16x4core nodes were needed with the config below, 8x for 1TB and below.
    // About 24hrs. for SF 1 to 10,000.
    if (scaleFactor.toInt >= 10000) {
      spark.conf.set("spark.sql.shuffle.partitions", "20000")
      SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.1")
    }
    else if (scaleFactor.toInt >= 1000) {
      spark.conf.set("spark.sql.shuffle.partitions", "2001") //one above 2000 to use HighlyCompressedMapStatus
      SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.3")
    }
    else {
      spark.conf.set("spark.sql.shuffle.partitions", "200") //default
      SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.5")
    }
  }

  def generateData(tables: Tables, location: String, scaleFactor: String, workers: Int, cores: Int): Unit = {
    def isPartitioned (tables: Tables, tableName: String) : Boolean =
      util.Try(tables.tables.find(_.name == tableName).get.partitionColumns.nonEmpty).getOrElse(false)

    val tableNames = tables.tables.map(_.name)
    tableNames.foreach { tableName =>
      // generate data
      time {
        tables.genData(
          location = location,
          format = fileFormat,
          overwrite = overwrite,
          partitionTables = true,
          // if to coallesce into a single file (only one writter for non partitioned tables = slow)
          clusterByPartitionColumns = if (isPartitioned(tables, tableName)) false else true,
          filterOutNullPartitionValues = false,
          tableFilter = tableName,
          // this controlls parallelism on datagen and number of writers (# of files for non-partitioned)
          // in general we want many writers to S3, and smaller tasks for large scale factors to avoid OOM and shuffle errors
          numPartitions = if (scaleFactor.toInt <= 100 || !isPartitioned(tables, tableName))
            workers * cores
          else
            workers * cores * 4
        )
      }
    }
  }

  def importData(spark: SparkSession, dbname: String, tables: Tables, location: String): Unit = {
    def createExternal(location: String, dbname: String, tables: Tables): Unit = {
      tables.createExternalTables(location, fileFormat, dbname, overwrite = overwrite, discoverPartitions = true)
    }

    val tableNames = tables.tables.map(_.name)
    time {
      println(s"Creating external tables at $location")
      createExternal(location, dbname, tables)
    }

    // Show table information and attempt to vacuum
    tableNames.foreach { tableName =>

      println(s"Table $tableName has " + util.Try {
        spark.sql(s"SHOW PARTITIONS $tableName").count() + " partitions"
      }.getOrElse(s"no partitions"))

      util.Try {
        spark.sql(s"VACUUM $tableName RETAIN 0.0. HOURS")
      } getOrElse println(s"Cannot VACUUM $tableName")

      spark.sql(s"DESCRIBE EXTENDED $tableName").show(999, truncate = false)

      println
    }
  }

}
