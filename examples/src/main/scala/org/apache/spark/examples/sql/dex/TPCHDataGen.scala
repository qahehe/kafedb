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

import java.nio.file.FileSystems
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.dex.DexBuilder.createTreeIndex
import org.apache.spark.sql.dex.DexConstants.TableAttribute
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.Utils
// For datagens
import java.io._
import scala.sys.process._


object TPCHDataGen {

  private def absPathOf(path: String): String = {
    val fs = FileSystems.getDefault
    fs.getPath(path).toAbsolutePath.toString
  }

  val dbUrl = "jdbc:postgresql://localhost:7433/test_db"
  val dbProps = {
    val p = new Properties()
    p.setProperty("Driver", "org.postgresql.Driver")
    p
  }

  val benchmark = "TPCH"
  //val scaleFactors = Seq("1", "10", "100", "1000", "10000") // "1", "10", "100", "1000", "10000" list of scale factors to generate and import
  //val scaleFactors = Seq("1") // "1", "10", "100", "1000", "10000" list of scale factors to generate and import
  val scaleFactor = "1"
  val baseLocation = absPathOf("bench/data")
  val baseDatagenFolder = absPathOf("bench/datagen")
  println(s"baseLocation=${baseLocation}")
  println(s"baseDatagenFolder=${baseDatagenFolder}")

  // Output file formats
  val fileFormat = "parquet" // only parquet was tested
  val shuffle = true // If true, partitions will be coalesced into a single file during generation up to spark.sql.files.maxRecordsPerFile (if set)
  val overwrite = false //if to delete existing files (doesn't check if results are complete on no-overwrite)

  // Generate stats for CBO
  val createTableStats = false
  val createColumnStats = false

  val dbSuffix = ""

  val tableNamesToDex = Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
  // p_partkey = ps_partkey
  // and s_suppkey = ps_suppkey
  //	and p_size = 15
  //	and p_type like '%BRASS'
  //	and s_nationkey = n_nationkey
  //	and n_regionkey = r_regionkey
  //	and r_name = 'EUROPE'
  val joinableAttrsToDex = Seq(
    (TableAttribute("part", "p_partkey"), TableAttribute("partsupp", "ps_partkey")),
    (TableAttribute("partsupp", "ps_suppkey"), TableAttribute("supplier", "s_suppkey")),
    (TableAttribute("nation", "n_nationkey"), TableAttribute("supplier", "s_nationkey")),
    (TableAttribute("nation", "n_regionkey"), TableAttribute("region", "r_regionkey")),
    //(TableAttribute("customer", "c_nationkey"), TableAttribute("supplier", "s_nationkey")),
    (TableAttribute("customer", "c_nationkey"), TableAttribute("nation", "n_nationkey")),
    (TableAttribute("customer", "c_custkey"), TableAttribute("orders", "o_custkey")),
    (TableAttribute("lineitem", "l_partkey"), TableAttribute("partsupp", "ps_partkey")),
    (TableAttribute("lineitem", "l_suppkey"), TableAttribute("partsupp", "ps_suppkey")),
    (TableAttribute("lineitem", "l_orderkey"), TableAttribute("orders", "o_orderkey"))
  )
  val filterAttrsToDex = Seq(
    TableAttribute("customer", "c_mktsegment"),
    TableAttribute("lineitem", "l_returnflag"),
    TableAttribute("lineitem", "l_shipmode"),
    TableAttribute("lineitem", "l_shipinstruct"),
    TableAttribute("nation", "n_name"),
    TableAttribute("orders", "o_orderpriority"),
    TableAttribute("orders", "o_orderstatus"),
    TableAttribute("part", "p_size"),
    TableAttribute("part", "p_type"),
    TableAttribute("part", "p_brand"),
    TableAttribute("part", "p_container"),
    TableAttribute("region", "r_name")
  )

  def newSparkSession(name: String): SparkSession = SparkSession
    .builder()
    .appName(name)
    .getOrCreate()

  private val modes = Set("tpch", "postgres", "dex")

  def main(args: Array[String]): Unit = {
    require(args.length >= 1)
    val neededModes = args(0) match {
      case "all" => modes
      case _ =>
        require(args.forall(modes.contains))
        args.toSet
    }
    println(s"neededModes=${neededModes.mkString(", ")}")

    val spark = newSparkSession("TPCH Data Generation")

    //val workers: Int = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt //number of nodes, assumes one executor per node
    val workers: Int = spark.sparkContext.getConf.getInt("spark.executor.instances", 1)
    println(s"workers=${workers}")

    //val workers: Int = 1
    val cores: Int = Runtime.getRuntime.availableProcessors //number of CPU-cores
    println(s"cores=${cores}")

    // Set Spark config to produce same and comparable source files across systems
    // do not change unless you want to derive from default source file composition, in that case also set a DB suffix
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    // Prevent very large files. 20 million records creates between 500 and 1500MB files in TPCH
    spark.sqlContext.setConf("spark.sql.files.maxRecordsPerFile", "20000000")  // This also regulates the file coalesce

    //waitForWorkers(spark, workers, 3600) //wait up to an hour

    // install (build) the data generators in all nodes
    val worker = 1l
    s"TPCH worker $worker\n" + installDBGEN(baseFolder = baseDatagenFolder)(worker)

    // Generate the data, import the tables, generate stats for selected benchmarks and scale factors

    // First set some config settings affecting OOMs/performance
    setScaleConfig(spark, scaleFactor)

    val (dbname, tables, location) = getBenchmarkData(spark, scaleFactor)

    // Generate data
    if (neededModes.contains("tpch")) {
      time {
        println(s"Generating data for $benchmark SF $scaleFactor at $location")
        generateData(tables, location, scaleFactor, workers, cores)
      }
    }

    // Point data to Spark
    time {
      println(s"\nPointing data for $benchmark to Spark database $dbname from $location")
      pointDataToSpark(spark, dbname, tables, location)
    }
    if (createTableStats) time {
      println(s"\nGenerating table statistics for DB $dbname (with analyzeColumns=$createColumnStats)")
      tables.analyzeTables(dbname, analyzeColumns = createColumnStats)
    }


    // Validate data in Spark
    //validate(spark, scaleFactor, dbname)

    if (neededModes.contains("postgres")) {
      time {
        println(s"\nLoading plaintext Postgres for $benchmark into Postgres from $location, overwrite=$overwrite")
        loadPlaintext(spark, tables, overwrite)
      }
    }

    if (neededModes.contains("dex")) {
      time {
        println(s"\nBuilding DEX for $benchmark into Postgres from $location")
        buildDex(spark, tables)
      }
    }


    spark.stop()
  }

  private def buildDex(spark: SparkSession, tables: TPCHTables): Unit = {
    // Encrypt data to Postgres
    val allTableNames = tables.tables.map(_.name).toSet
    assert(tableNamesToDex.forall(allTableNames.contains))
    assert(joinableAttrsToDex.map(_._1.table).forall(allTableNames.contains))
    assert(joinableAttrsToDex.map(_._2.table).forall(allTableNames.contains))

    val nameToDfForDex = tableNamesToDex.map { t =>
      t -> spark.table(t)
    }.toMap

    spark.sessionState.dexBuilder.buildFromData(nameToDfForDex, joinableAttrsToDex)
  }

  private def loadPlaintext(spark: SparkSession, tables: TPCHTables, overwrite: Boolean): Unit = {
    tables.tables.map(_.name).foreach { t =>
      val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore
      spark.table(t).write.mode(saveMode).jdbc(dbUrl, t, dbProps)
    }

    Utils.classForName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(dbUrl, dbProps)
    try {
      filterAttrsToDex.foreach { f =>
        createTreeIndex(conn, f.table, spark.table(f.table), f.attr)
      }
      joinableAttrsToDex.foreach { case (j1, j2) =>
        createTreeIndex(conn, j1.table, spark.table(j1.table), j1.attr)
        createTreeIndex(conn, j2.table, spark.table(j2.table), j2.attr)
      }

      if (overwrite) {
        conn.prepareStatement("analyze").execute()
      }
    } finally {
      conn.close()
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

  def installDBGEN(url: String = "https://github.com/zheguang/tpch-dbgen.git",
                   //useStdout: Boolean = false,
                   baseFolder: String = "/tmp")(i: java.lang.Long): String = {
    // check if we want the revision which makes dbgen output to stdout
    //val checkoutRevision: String = if (useStdout) "git checkout 0469309147b42abac8857fa61b4cf69a6d3128a8 -- bm_utils.c" else ""

    Seq("mkdir", "-p", baseFolder).!
    val pw = new PrintWriter(new File(s"$baseFolder/dbgen_$i.sh" ))
    pw.write(
      s"""
         |rm -rf $baseFolder/dbgen
         |rm -rf $baseFolder/dbgen_install_$i
         |mkdir $baseFolder/dbgen_install_$i
         |(cd $baseFolder/dbgen_install_$i && git clone '$url' && cd tpch-dbgen && make)
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
    require(requiredWorkers > 0)
    def getNumWorkers(): Int =
      spark.sparkContext.getExecutorMemoryStatus.size - 1

    for (i <- 0 until tries) {
      val numWorkers = getNumWorkers()
      println(s"numWorkers=${numWorkers}")
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
        useStringForDate = true,
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

  def pointDataToSpark(spark: SparkSession, dbname: String, tables: Tables, location: String): Unit = {
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
