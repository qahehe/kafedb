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

import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID
import org.apache.spark.sql.dex.DexBuilder.{JoinableAttrs, TableAttribute, TableName}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, lit, monotonically_increasing_id, posexplode, udf}
import org.apache.spark.sql.internal.SQLConf

object DexBuilder {
  type TableName = String
  type AttrName = String
  case class TableAttribute(table: TableName, attr: AttrName)
  type JoinableAttrs = (TableAttribute, TableAttribute)
}

class DexBuilder(session: SparkSession) extends Serializable with Logging {
  // take a csv file and load it into dataframe
  // take a dataframe, encrypt it, and load it into remote postgres.  Store keys into sessionCatalog.
  import session.sqlContext.implicits._

  private val encDbUrl = SQLConf.get.dexEncryptedDataSourceUrl
  private val encDbProps = {
    val p = new Properties()
    p.setProperty("Driver", "org.postgresql.Driver")
    p
  }

  private val tFilterName = "t_filter"
  private val tCorrJoinName = "t_correlated_join"

  private val encKey: String = "enc"
  private val prfKey: String = "prf"

  private val udfCell = udf(encryptCell(encKey) _)

  private val udfLabel = udf { (predicate: String, counter: Int) =>
    labelFrom(prfKey)(predicate, counter)
  }
  private val udfValue = udf(encryptCell(encKey) _)

  private val udfRandPred = udf(randomPredicate(prfKey) _)

  def buildFromData(nameToDf: Map[TableName, DataFrame], joins: Seq[JoinableAttrs]): Unit = {
    val nameToRidDf = nameToDf.map { case (n, d) =>
      n -> d.withColumn("rid", monotonically_increasing_id).cache()
    }

    val encNameToEncDf = nameToRidDf.map { case (n, r) =>
      encryptTable(n, r)
    }

    val tFilterDfParts = nameToRidDf.flatMap { case (n, r) =>
        tFilterPartForTable(n, r)
    }
    val tFilterDf = tFilterDfParts.reduce((d1, d2) => d1 union d2)

    val tCorrJoinDfParts = for {
      (attrLeft, attrRight) <- joins
    } yield {
      val udfJoinPred = udf(joinPredicateOf(attrLeft, attrRight) _)
      val (dfLeft, dfRight) = (nameToRidDf(attrLeft.table), nameToRidDf(attrRight.table))
      dfLeft.withColumnRenamed("rid", "rid_left").join(dfRight.withColumnRenamed("rid", "rid_right"), col(attrLeft.attr) === col(attrRight.attr))
        .groupBy("rid_left").agg(collect_list($"rid_right").as("rids_right"))
        .select($"rid_left", posexplode($"rids_right").as("counter" :: "rid_right" :: Nil))
        .withColumn("predicate", udfRandPred(udfJoinPred($"rid_left")))
        .withColumn("label", udfLabel($"predicate", $"counter"))
        .withColumn("value", udfValue($"rid_right"))
        .select("label", "value")
    }
    val tCorrJoinDf = tCorrJoinDfParts.reduce((d1, d2) => d1 union d2)

    encNameToEncDf.foreach { case (n, e) =>
        e.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, n, encDbProps)
    }
    tFilterDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, tFilterName, encDbProps)
    tCorrJoinDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, tCorrJoinName, encDbProps)
  }

  private def encryptTable(table: TableName, ridDf: DataFrame): (String, DataFrame) = {
    require(ridDf.columns.contains("rid"))
    val colToRandCol = ridDf.columns.collect {
      case c if c == "rid" => "rid" -> "rid"
      case c => c -> randomId(encKey, c)
    }.toMap
    val ridDfProject = colToRandCol.values.map(col).toSeq

    (randomId(prfKey, table),
      ridDf.columns.foldLeft(ridDf) { case (d, c) =>
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

  private def filterPredicateOf(table: String, column: String)(value: String): String = {
    s"$table~$column~$value"
  }

  private def labelFrom(prfKey: String)(predicate: String, counter: Int): String = {
    s"$predicate~$counter"
  }

  private def encryptCell(key: String)(cell: String): String = {
    s"${cell}_$key"
  }
}
