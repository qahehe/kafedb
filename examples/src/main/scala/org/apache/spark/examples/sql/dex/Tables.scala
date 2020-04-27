/*
Copyright 2020, Brown University, Providence, RI.

                        All Rights Reserved

Permission to use, copy, modify, and distribute this software and
its documentation for any purpose other than its incorporation into a
commercial product or service is hereby granted without fee, provided
that the above copyright notice appear in all copies and that both
that copyright notice and this permission notice appear in supporting
documentation, and that the name of Brown University not be used in
advertising or publicity pertaining to distribution of the software
without specific, written prior permission.

BROWN UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR ANY
PARTICULAR PURPOSE.  IN NO EVENT SHALL BROWN UNIVERSITY BE LIABLE FOR
ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package org.apache.spark.examples.sql.dex
// scalastyle:off

import org.slf4j.LoggerFactory

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}


abstract class Tables(sqlContext: SQLContext, scaleFactor: String,
                      useDoubleForDecimal: Boolean = false, useStringForDate: Boolean = false)
  extends Serializable {

  def dataGenerator: DataGenerator
  def tables: Seq[Table]

  private val log = LoggerFactory.getLogger(getClass)

  def sparkContext: SparkContext = sqlContext.sparkContext

  case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
    val schema = StructType(fields)

    def nonPartitioned: Table = {
      Table(name, Nil, fields : _*)
    }

    /**
     *  If convertToSchema is true, the data from generator will be parsed into columns and
     *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
     */
    def df(convertToSchema: Boolean, numPartition: Int) = {
      val generatedData = dataGenerator.generate(sparkContext, name, numPartition, scaleFactor)
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def convertTypes(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType if useDoubleForDecimal => DoubleType
          case date: DateType if useStringForDate => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, newFields:_*)
    }

    def genData(
                 location: String,
                 format: String,
                 overwrite: Boolean,
                 clusterByPartitionColumns: Boolean,
                 filterOutNullPartitionValues: Boolean,
                 numPartitions: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(format != "text", numPartitions)
      val tempTableName = s"${name}_text"
      data.createOrReplaceTempView(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          println(s"Pre-clustering with partitioning columns with query $query.")
          log.info(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // treat non-partitioned tables as "one partition" that we want to coalesce
        if (clusterByPartitionColumns) {
          // in case data has more than maxRecordsPerFile, split into multiple writers to improve datagen speed
          // files will be truncated to maxRecordsPerFile value, so the final result will be the same
          val numRows = data.count
          val maxRecordPerFile = util.Try(sqlContext.getConf("spark.sql.files.maxRecordsPerFile").toInt).getOrElse(0)

          println(s"Data has $numRows rows clustered $clusterByPartitionColumns for $maxRecordPerFile")
          log.info(s"Data has $numRows rows clustered $clusterByPartitionColumns for $maxRecordPerFile")

          if (maxRecordPerFile > 0 && numRows > maxRecordPerFile) {
            val numFiles = (numRows.toDouble/maxRecordPerFile).ceil.toInt
            println(s"Coalescing into $numFiles files")
            log.info(s"Coalescing into $numFiles files")
            data.coalesce(numFiles).write
          } else {
            data.coalesce(1).write
          }
        } else {
          data.write
        }
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns : _*)
      }
      println(s"Generating table $name in database to $location with save mode $mode.")
      log.info(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }

    def createExternalTable(location: String, format: String, databaseName: String,
                            overwrite: Boolean, discoverPartitions: Boolean = true): Unit = {

      val qualifiedTableName = databaseName + "." + name
      val tableExists = sqlContext.tableNames(databaseName).contains(name)
      if (overwrite) {
        sqlContext.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
      }
      if (!tableExists || overwrite) {
        println(s"Creating external table $name in database $databaseName using data stored in $location.")
        log.info(s"Creating external table $name in database $databaseName using data stored in $location.")
        sqlContext.createExternalTable(qualifiedTableName, location, format)
      }
      if (partitionColumns.nonEmpty && discoverPartitions) {
        println(s"Discovering partitions for table $name.")
        log.info(s"Discovering partitions for table $name.")
        sqlContext.sql(s"ALTER TABLE $databaseName.$name RECOVER PARTITIONS")
      }
    }

    def createTemporaryTable(location: String, format: String): Unit = {
      println(s"Creating temporary table $name using data stored in $location.")
      log.info(s"Creating temporary table $name using data stored in $location.")
      sqlContext.read.format(format).load(location).createOrReplaceTempView(name)
    }

    def analyzeTable(databaseName: String, analyzeColumns: Boolean = false): Unit = {
      println(s"Analyzing table $name.")
      log.info(s"Analyzing table $name.")
      sqlContext.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS")
      if (analyzeColumns) {
        val allColumns = fields.map(_.name).mkString(", ")
        println(s"Analyzing table $name columns $allColumns.")
        log.info(s"Analyzing table $name columns $allColumns.")
        sqlContext.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS FOR COLUMNS $allColumns")
      }
    }
  }

  def genData(
               location: String,
               format: String,
               overwrite: Boolean,
               partitionTables: Boolean,
               clusterByPartitionColumns: Boolean,
               filterOutNullPartitionValues: Boolean,
               tableFilter: String = "",
               numPartitions: Int = 100): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    tablesToBeGenerated.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues, numPartitions)
    }
  }

  def createExternalTables(location: String, format: String, databaseName: String,
                           overwrite: Boolean, discoverPartitions: Boolean, tableFilter: String = ""): Unit = {

    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }

    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createExternalTable(tableLocation, format, databaseName, overwrite, discoverPartitions)
    }
    sqlContext.sql(s"USE $databaseName")
    println(s"The current database has been set to $databaseName.")
    log.info(s"The current database has been set to $databaseName.")
  }

  def createTemporaryTables(location: String, format: String, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createTemporaryTable(tableLocation, format)
    }
  }

  def analyzeTables(databaseName: String, analyzeColumns: Boolean = false, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      table.analyzeTable(databaseName, analyzeColumns)
    }
  }
}