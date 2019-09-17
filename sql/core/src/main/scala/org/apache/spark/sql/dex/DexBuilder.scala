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
package org.apache.spark.sql.dex
// scalastyle:off

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.dex.DexBuilder.createTreeIndex
import org.apache.spark.sql.dex.DexConstants.{JoinableAttrs, TableAttribute, TableName}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, lit, monotonically_increasing_id, posexplode, row_number, udf}
import org.apache.spark.util.Utils

object DexBuilder {
  def createHashIndex(conn: Connection, tableName: String, df: DataFrame, col: String): Unit = {
    conn.prepareStatement(
      s"""
         |CREATE INDEX IF NOT EXISTS "${tableName}_${col}_hash"
         |ON "$tableName"
         |USING HASH ($col)
      """.stripMargin).executeUpdate()
  }

  def createTreeIndex(conn: Connection, tableName: String, df: DataFrame, col: String): Unit = {
    conn.prepareStatement(
      s"""
         |CREATE INDEX IF NOT EXISTS "${tableName}_${col}_tree"
         |ON "$tableName"
         |($col)
      """.stripMargin).executeUpdate()
  }
}

class DexBuilder(session: SparkSession) extends Serializable with Logging {
  // take a csv file and load it into dataframe
  // take a dataframe, encrypt it, and load it into remote postgres.  Store keys into sessionCatalog.
  import session.sqlContext.implicits._

  //private val encDbUrl = SQLConf.get.dexEncryptedDataSourceUrl
  private val encDbUrl = "jdbc:postgresql://localhost:8433/test_edb"
  private val encDbProps = {
    val p = new Properties()
    p.setProperty("Driver", "org.postgresql.Driver")
    p
  }

  private val encKey: String = "enc"
  private val prfKey: String = "prf"

  private val udfCell = udf(encryptCell(encKey) _)

  private val udfLabel = udf { (predicate: String, counter: Int) =>
    labelFrom(prfKey)(predicate, counter)
  }
  private val udfValue = udf(encryptCell(encKey) _)

  private val udfRandPred = udf(randomPredicate(prfKey) _)

  private val udfRid = udf((rid: Number) => s"$rid")

  def buildFromData(nameToDf: Map[TableName, DataFrame], joins: Seq[JoinableAttrs]): Unit = {
    val nameToRidDf = nameToDf.map { case (n, d) =>
      n -> d.withColumn("rid", monotonically_increasing_id()).cache()
    }

    val encNameToEncDf = nameToRidDf.map { case (n, r) =>
      encryptTable(n, r)
    }

    val tFilterDfParts = nameToRidDf.flatMap { case (n, r) =>
      r.columns.filterNot(_ == "rid").map { c =>
        val udfPredicate = udf(filterPredicateOf(n, c) _)
        r.withColumn("counter", row_number().over(Window.partitionBy(c).orderBy(c)) - 1).repartition(col(c))
          //.groupBy(c).agg(collect_list($"rid").as("rids"))
          //.select(col(c), posexplode($"rids").as("counter" :: "rid" :: Nil))
          .withColumn("predicate", udfRandPred(udfPredicate(col(c))))
          .withColumn("label", udfLabel($"predicate", $"counter"))
          .withColumn("value", udfValue($"rid"))
          .select("label", "value")
      }
    }
    val tFilterDf = tFilterDfParts.reduce((d1, d2) => d1 union d2)

    val tDomainDfParts = nameToRidDf.flatMap { case (n, r) =>
      r.columns.filterNot(_ == "rid").map { c =>
        r.select(col(c)).distinct()
          .withColumn("counter", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1).repartition(col(c))
          .withColumn("predicate", lit(domainPredicateOf(n, c)))
          .withColumn("label", udfLabel($"predicate", $"counter"))
          .withColumn("value", udfValue(col(c)))
          .select("label", "value").repartition($"label")
      }
    }
    val tDomainDf = tDomainDfParts.reduce((d1, d2) => d1 union d2)

    val tUncorrJoinDfParts = for {
      (attrLeft, attrRight) <- joins
    } yield {
      val (dfLeft, dfRight) = (nameToRidDf(attrLeft.table), nameToRidDf(attrRight.table))
      dfLeft.withColumnRenamed("rid", "rid_left")
        .join(dfRight.withColumnRenamed("rid", "rid_right"), col(attrLeft.attr) === col(attrRight.attr))
        .withColumn("counter", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1).repartition($"counter")
        .withColumn("predicate", lit(uncorrJoinPredicateOf(attrLeft, attrRight)))
        .withColumn("label", udfLabel($"predicate", $"counter"))
        .withColumn("value_left", udfValue($"rid_left"))
        .withColumn("value_right", udfValue($"rid_right"))
        .select("label", "value_left", "value_right")
    }
    val tUncorrJoinDf = tUncorrJoinDfParts.reduce((d1, d2) => d1 union d2)

    val tCorrJoinDfParts = for {
      (attrLeft, attrRight) <- joins
    } yield {
      val udfJoinPred = udf(joinPredicateOf(attrLeft, attrRight) _)
      val (dfLeft, dfRight) = (nameToRidDf(attrLeft.table), nameToRidDf(attrRight.table))
      dfLeft.withColumnRenamed("rid", "rid_left").join(dfRight.withColumnRenamed("rid", "rid_right"), col(attrLeft.attr) === col(attrRight.attr))
        //.groupBy("rid_left").agg(collect_list($"rid_right").as("rids_right"))
        //.select($"rid_left", posexplode($"rids_right").as("counter" :: "rid_right" :: Nil))
        .withColumn("counter", row_number().over(Window.partitionBy("rid_left").orderBy("rid_left")) - 1).repartition($"counter")
        .withColumn("predicate", udfRandPred(udfJoinPred($"rid_left")))
        .withColumn("label", udfLabel($"predicate", $"counter"))
        .withColumn("value", udfValue($"rid_right"))
        .select("label", "value")
    }
    val tCorrJoinDf = tCorrJoinDfParts.reduce((d1, d2) => d1 union d2)

    encNameToEncDf.foreach { case (n, e) =>
      e.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, n, encDbProps)
    }
    tFilterDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tFilterName, encDbProps)
    tDomainDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tDomainName, encDbProps)
    tUncorrJoinDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tUncorrJoinName, encDbProps)
    tCorrJoinDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tCorrJoinName, encDbProps)

    Utils.classForName("org.postgresql.Driver")
    val encConn = DriverManager.getConnection(encDbUrl, encDbProps)
    try {
      encNameToEncDf.foreach { case (n, e) =>
        createTreeIndex(encConn, n, e, "rid")
      }
      createTreeIndex(encConn, DexConstants.tFilterName, tFilterDf, "label")
      createTreeIndex(encConn, DexConstants.tDomainName, tDomainDf, "label")
      createTreeIndex(encConn, DexConstants.tUncorrJoinName, tUncorrJoinDf, "label")
      createTreeIndex(encConn, DexConstants.tCorrJoinName, tCorrJoinDf, "label")

      encConn.prepareStatement("analyze").execute()
    } finally {
      encConn.close()
    }
  }

  private def encryptTable(table: TableName, ridDf: DataFrame): (String, DataFrame) = {
    require(ridDf.columns.contains("rid"))
    val colToRandCol = ridDf.columns.collect {
      case c if c == "rid" => "rid" -> "rid"
      case c => c -> randomId(prfKey, c)
    }.toMap
    val ridDfProject = colToRandCol.values.map(col).toSeq

    (randomId(prfKey, table),
      ridDf.columns.foldLeft(ridDf) {
        case (d, c) if c == "rid" =>
          d.withColumn(colToRandCol(c), udfRid(col(c)))
        case (d, c) =>
          d.withColumn(colToRandCol(c), udfCell(col(c)))
      }.select(ridDfProject: _*))
  }

  private def tFilterPartForTable(table: TableName, ridDf: DataFrame): Seq[DataFrame] = {
    ridDf.columns.filterNot(_ == "rid").map { c =>
      val udfPredicate = udf(filterPredicateOf(table, c) _)

      ridDf.groupBy(c).agg(collect_list($"rid").as("rids"))
        .select(col(c), posexplode($"rids").as("counter" :: "rid" :: Nil))
        .withColumn("predicate", udfRandPred(udfPredicate(col(c))))
        .withColumn("label", udfLabel($"predicate", $"counter"))
        .withColumn("value", udfValue($"rid"))
        .select("label", "value")
    }
  }

  private def randomId(prfKey: String, ident: String): String = {
    s"${ident}_$prfKey"
  }

  private def encryptId(encKey: String, ident: String): String = {
    s"${ident}_$encKey"
  }

  private def randomPredicate(prfKey: String)(predicate: String): String = {
    // todo: use prfkey
    predicate
  }

  private def joinPredicateOf(attrLeft: TableAttribute, attrRight: TableAttribute)(ridLeft: String): String = {
    s"${attrLeft.table}~${attrLeft.attr}~${attrRight.table}~${attrRight.attr}~$ridLeft"
  }

  private def uncorrJoinPredicateOf(attrLeft: TableAttribute, attrRight: TableAttribute): String = {
    s"${attrLeft.table}~${attrLeft.attr}~${attrRight.table}~${attrRight.attr}"
  }

  private def filterPredicateOf(table: String, column: String)(value: String): String = {
    s"$table~$column~$value"
  }

  private def domainPredicateOf(table: String, column: String): String = {
    s"$table~$column"
  }

  private def labelFrom(prfKey: String)(predicate: String, counter: Int): String = {
    s"$predicate~$counter"
  }

  private def encryptCell(key: String)(cell: String): String = {
    s"${cell}_$key"
  }
}
