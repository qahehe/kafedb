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
import org.apache.spark.sql.dex.DexBuilder.{ForeignKey, PrimaryKey, createTreeIndex}
import org.apache.spark.sql.dex.DexConstants._
import org.apache.spark.sql.dex.DexPrimitives._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.util.Utils

object DexVariant {
  def from(str: String): DexVariant = str.toLowerCase match {
    case "dexspx" => DexSpx
    case "dexcorr" => DexCorr
    case "dexpkfk" => DexPkFk
  }
}
sealed trait DexVariant {
  def name: String = getClass.getSimpleName
}
sealed trait DexStandalone
case object DexSpx extends DexVariant with DexStandalone
case object DexCorr extends DexVariant with DexStandalone
case object DexPkFk extends DexVariant

object DexBuilder {
  def createHashIndex(conn: Connection, tableName: String, col: String): Unit = {
    conn.prepareStatement(
      s"""
         |CREATE INDEX IF NOT EXISTS "${tableName}_${col}_hash"
         |ON "$tableName"
         |USING HASH ($col)
      """.stripMargin).executeUpdate()
  }

  def createTreeIndex(conn: Connection, tableName: String, col: String): Unit = {
    conn.prepareStatement(
      s"""
         |CREATE INDEX IF NOT EXISTS "${tableName}_${col}_tree"
         |ON "$tableName"
         |($col)
      """.stripMargin).executeUpdate()
  }

  case class PrimaryKey(attr: TableAttribute)
  case class ForeignKey(attr: TableAttribute, ref: TableAttribute)
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

  private val udfCell = udf(dexCellOf _)

  private val udfEmmLabel = udf { (dexPredicate: String, counter: Int) =>
    dexEmmLabelOf(dexPredicate, counter)
  }
  private val udfEmmValue = udf { (dexPredicate: String, rid: Long) =>
    dexEmmValueOf(dexPredicate, rid)
  }

  private val udfRid = udf { rid: Long =>
    dexRidOf(rid)
  }

  private def pibasCounterOn(c: Column): Column = row_number().over(Window.partitionBy(c).orderBy(c)) - 1

  private def primaryKeyAndForeignKeysFor(t: TableName, primaryKeys: Set[PrimaryKey], foreignKeys: Set[ForeignKey]): (PrimaryKey, Set[ForeignKey]) = {
    val pk = {
      val pks = primaryKeys.filter(_.attr.table == t)
      require(pks.size == 1, s"$t doesn't have a primary key: every table should have a primary key, atom or compound")
      pks.headOption.get
    }
    val fks = foreignKeys.filter(_.attr.table == t)
    (pk, fks)
  }

  private def compoundKeyCol(c: TableAttributeCompound): Column = {
    /* doesn't work: 2a + 3b != 2a + 3b + 5c, but just using (2, 3) will equate them
    def sieve(s: Stream[Int]): Stream[Int] = {
      s.head #:: sieve(s.tail.filter(_ % s.head != 0))
    }*/
    //val primes = sieve(Stream.from(2))
    //val uniquePrimeFactorization = c.attrs.map(col).zip(primes).map(x => x._1 * x._2).reduce((x, y) => x + y)
    //uniquePrimeFactorization
    // Use '&' to distinguish 1 || 12 collision with 11 || 2
    //concat(c.attrs.flatMap(x => Seq(col(x), lit("_and_"))).dropRight(1): _*)
    def cantorPairing(a: Column, b: Column): Column = {
      // https://en.wikipedia.org/wiki/Pairing_function
      (a + b) * (a + b + 1) / 2 + b
    }
    require(c.attrs.size == 2, "only length-2 compound key is supported")
    val cols = c.attrs.map(col)
    cantorPairing(cols.head, cols(1)).cast(LongType) // unique identifier for ordered pair of numbers
  }

  private def pkCol(pk: PrimaryKey): Column = pk.attr match {
    case a: TableAttributeAtom => col(a.attr)
    case c: TableAttributeCompound => compoundKeyCol(c)
  }

  private def encColName(t: TableName, c: AttrName): String = s"${c}_prf"

  def buildPkFkSchemeFromData(nameToDf: Map[TableName, DataFrame],
                              primaryKeys: Set[PrimaryKey],
                              foreignKeys: Set[ForeignKey]): Unit = {

    nameToDf.foreach {
      case (t, d) =>
        val (pk, fks) = primaryKeyAndForeignKeysFor(t, primaryKeys, foreignKeys)

        def pkColName(pk: PrimaryKey): String = "rid"
        def pfkColName(fk: ForeignKey): String = s"pfk_${fk.ref.table}_${fk.attr.table}_prf"
        def fpkColName(fk: ForeignKey): String = s"fpk_${fk.attr.table}_${fk.ref.table}_prf"
        def valColName(t: TableName, c: AttrName): String = s"val_${t}_${c}_prf"

        /*def outputCols(d: DataFrame, pk: PrimaryKey, fks: Set[ForeignKey]): Seq[AttrName] = d.columns.flatMap {
          case c if c == pk.attr.attr =>
            Seq(pkColName(pk))
          case c if fks.map(_.attr.attr).contains(c) =>
            val fk = fks.find(_.attr.attr == c).get
            Seq(pfkColName(fk), fpkColName(fk))
          case c =>
            Seq(valColName(t, c), encColName(t, c))
        }*/

        def encIndexColNamesOf(d: DataFrame, pk: PrimaryKey, fks: Set[ForeignKey]): Seq[AttrName] = {
          Seq(pkColName(pk)) ++
            fks.flatMap(fk => Seq(pfkColName(fk), fpkColName(fk))) ++
            d.columns.collect {
              case c if nonKey(pk, fks, c) => valColName(t, c)
            }
        }
        def encDataColNamesOf(t: TableName, d: DataFrame, pk: PrimaryKey, fks: Set[ForeignKey]): Seq[AttrName] = d.columns.collect {
          case c if nonKey(pk, fks, c) =>
            encColName(t, c)
        }

        def pfkCol(fk: ForeignKey): Column = {
          def labelOf(c: Column): Column = concat(
            lit(fk.ref.table), lit("~"), lit(fk.attr.table), lit("~"), c, lit("~"),
            pibasCounterOn(c)
          )
          fk.attr match {
            case a: TableAttributeAtom => labelOf(col(a.attr))
            case c: TableAttributeCompound => labelOf(compoundKeyCol(c))
          }
        }
        def fpkCol(fk: ForeignKey, pk: PrimaryKey): Column = {
          def labelOf(c: Column): Column = concat(
            c, lit("_enc_"),
            lit(fk.attr.table), lit("~"), lit(fk.ref.table), lit("~"),
            col(pkColName(pk))
          )
          fk.attr match {
            case a: TableAttributeAtom => labelOf(col(a.attr))
            case c: TableAttributeCompound => labelOf(compoundKeyCol(c))
          }
        }
        def valCol(t: TableName, c: AttrName): Column = concat(
          lit(t), lit("~"), lit(c), lit("~"), col(c), lit("~"),
          pibasCounterOn(col(c))
        )
        def encCol(t: TableName, c: AttrName): Column = concat(col(c), lit("_enc"))

        val pkDf = d.withColumn(pkColName(pk), pkCol(pk))
        val pkfkDf = fks.foldLeft(pkDf) { case (pd, fk) =>
          pd.withColumn(pfkColName(fk), pfkCol(fk))
            .withColumn(fpkColName(fk), fpkCol(fk, pk))
        }

        val pkfkEncDf = d.columns.filterNot(
          c => c == pk.attr.attr || fks.map(_.attr.attr).contains(c)
        ).foldLeft(pkfkDf) {
          case (pd, c) =>
            pd.withColumn(valColName(t, c), valCol(t, c))
            .withColumn(encColName(t, c), encCol(t, c))
        }

        def encTableNameOf(t: TableName): String = s"${t}_prf"

        val encTableName = encTableNameOf(t)
        val encIndexColNames = encIndexColNamesOf(d, pk, fks)
        val encDataColNames = encDataColNamesOf(t, d, pk, fks)
        pkfkEncDf.selectExpr(encIndexColNames ++ encDataColNames:_*)
          .write.mode(SaveMode.Overwrite).jdbc(encDbUrl, encTableName, encDbProps)

        Utils.classForName("org.postgresql.Driver")
        val encConn = DriverManager.getConnection(encDbUrl, encDbProps)
        try {
          encIndexColNames.foreach { c =>
            createTreeIndex(encConn, encTableName, c)
          }
          //encConn.prepareStatement(s"analyze $encTableName").execute()
        } finally {
          encConn.close()
        }
    }

    Utils.classForName("org.postgresql.Driver")
    val encConn = DriverManager.getConnection(encDbUrl, encDbProps)
    try {
      encConn.prepareStatement(s"analyze").execute()
    } finally {
      encConn.close()
    }
  }

  private def nonKey(pk: PrimaryKey, fks: Set[ForeignKey], c: String): Boolean = {
    val nonPk = pk match {
      case PrimaryKey(attr: TableAttributeAtom) => c != attr.attr
      case PrimaryKey(attr: TableAttributeCompound) => c != attr.attr && !attr.attrs.contains(c)
    }
    val nonFk = fks.forall {
      case ForeignKey(attr: TableAttributeAtom, attrRef: TableAttributeAtom) =>
        c != attr.attr && c != attrRef.attr
      case ForeignKey(attr: TableAttributeCompound, attrRef: TableAttributeCompound) =>
        c != attr.attr && !attr.attrs.contains(c) && c != attrRef.attr && !attrRef.attrs.contains(c)
      case _ => throw DexException("unsupported")
    }
    nonPk && nonFk
  }

  def buildFromData(dexStandaloneVariant: DexStandalone, nameToDf: Map[TableName, DataFrame], primaryKeys: Set[PrimaryKey], foreignKeys: Set[ForeignKey]): Unit = {
    val nameToRidDf = nameToDf.map { case (n, d) =>
      val ridDf = d.withColumn("rid", monotonically_increasing_id()).cache()
      val (pk, fks) = primaryKeyAndForeignKeysFor(n, primaryKeys, foreignKeys)
      val ridPkDf = pk.attr match {
        case c: TableAttributeCompound =>
          ridDf.withColumn(c.attr, pkCol(pk))
        case a: TableAttributeAtom =>
          ridDf
      }
      val ridPkFkDf = fks.foldLeft(ridPkDf) {
        case (df, fk) => fk.attr match {
          case c: TableAttributeCompound =>
            df.withColumn(c.attr, compoundKeyCol(c))
          case a: TableAttributeAtom =>
            df
        }
      }
      n -> ridPkFkDf
    }

    def buildEncRidTables(): Unit = {
      val encNameToEncDf = nameToRidDf.map { case (n, r) =>
        encryptTable(n, r)
      }
      encNameToEncDf.foreach { case (n, e) =>
        e.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, n, encDbProps)
        buildIndexFor(n, DexConstants.ridCol)
      }
    }


    def buildTFilter(): Unit = {
      val tFilterDfParts = nameToRidDf.flatMap { case (n, r) =>
        val (pk, fks) = primaryKeyAndForeignKeysFor(n, primaryKeys, foreignKeys)
        r.columns.filter(c => nonKey(pk, fks, c) && c != DexConstants.ridCol).map { c =>
          val udfFilterPredicate = udf(dexPredicatesConcat(dexFilterPredicatePrefixOf(n, c)) _)
          r.withColumn("counter", row_number().over(Window.partitionBy(c).orderBy(c)) - 1).repartition(col(c))
            .withColumn("predicate", udfFilterPredicate(col(c)))
            .withColumn("label", udfEmmLabel($"predicate", $"counter"))
            .withColumn("value", udfEmmValue($"predicate", $"rid"))
            .select("label", "value")
        }
      }
      val tFilterDf = tFilterDfParts.reduce((d1, d2) => d1 union d2)
      tFilterDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tFilterName, encDbProps)
      buildIndexFor(DexConstants.tFilterName, DexConstants.emmLabelCol)
    }

    /*val tDomainDfParts = nameToRidDf.flatMap { case (n, r) =>
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
    tDomainDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tDomainName, encDbProps)
    buildIndexFor(Seq(DexConstants.tDomainName))
    */


    def buildTUncorrelatedJoin(): Unit = {
      val tUncorrJoinDfParts = for {
        ForeignKey(attr, attrRef) <- foreignKeys
        (attrLeft, attrRight) = if (attr.qualifiedName <= attrRef.qualifiedName) (attr, attrRef) else (attrRef, attr)
      } yield {
        val (dfLeft, dfRight) = (nameToRidDf(attrLeft.table), nameToRidDf(attrRight.table))
        val uncorrJoinPredicate = dexUncorrJoinPredicateOf(attrLeft, attrRight)
        dfLeft.withColumnRenamed("rid", "rid_left")
          .join(dfRight.withColumnRenamed("rid", "rid_right"), col(attrLeft.attr) === col(attrRight.attr))
          .withColumn("counter", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1).repartition($"counter")
          .withColumn("predicate", lit(uncorrJoinPredicate))
          .withColumn("label", udfEmmLabel($"predicate", $"counter"))
          .withColumn("value_left", udfEmmValue($"predicate", $"rid_left"))
          .withColumn("value_right", udfEmmValue($"predicate", $"rid_right"))
          .select("label", "value_left", "value_right")
      }
      val tUncorrJoinDf = tUncorrJoinDfParts.reduce((d1, d2) => d1 union d2)
      tUncorrJoinDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tUncorrJoinName, encDbProps)
      buildIndexFor(DexConstants.tUncorrJoinName, DexConstants.emmLabelCol)
    }

    def buildTCorrelatedJoin(): Unit = {
      val tCorrJoinDfParts = for {
        ForeignKey(attr, attrRef) <- foreignKeys
      } yield {
        def joinFor(attrLeft: TableAttribute, attrRight: TableAttribute): DataFrame = {
          val udfJoinPred = udf(dexPredicatesConcat(dexCorrJoinPredicatePrefixOf(attrLeft, attrRight)) _)
          val (dfLeft, dfRight) = (nameToRidDf(attrLeft.table), nameToRidDf(attrRight.table))
          require(dfLeft.columns.contains(attrLeft.attr) && dfRight.columns.contains(attrRight.attr), s"${attrLeft.attr} and ${attrRight.attr}")
          dfLeft.withColumnRenamed("rid", "rid_left").join(dfRight.withColumnRenamed("rid", "rid_right"), col(attrLeft.attr) === col(attrRight.attr))
            //.groupBy("rid_left").agg(collect_list($"rid_right").as("rids_right"))
            //.select($"rid_left", posexplode($"rids_right").as("counter" :: "rid_right" :: Nil))
            .withColumn("counter", row_number().over(Window.partitionBy("rid_left").orderBy("rid_left")) - 1).repartition($"counter")
            .withColumn("predicate", udfJoinPred($"rid_left"))
            .withColumn("label", udfEmmLabel($"predicate", $"counter"))
            .withColumn("value", udfEmmValue($"predicate", $"rid_right"))
            .select("label", "value")
        }
        joinFor(attr, attrRef) union joinFor(attrRef, attr)
      }
      val tCorrJoinDf = tCorrJoinDfParts.reduce((d1, d2) => d1 union d2)
      tCorrJoinDf.write.mode(SaveMode.Overwrite).jdbc(encDbUrl, DexConstants.tCorrJoinName, encDbProps)
      buildIndexFor(DexConstants.tCorrJoinName, DexConstants.emmLabelCol)
    }

    def buildIndexFor(encName: String, col: String): Unit = {
      Utils.classForName("org.postgresql.Driver")
      val encConn = DriverManager.getConnection(encDbUrl, encDbProps)
      try {
          createTreeIndex(encConn, encName, col)
      } finally {
        encConn.close()
      }
    }

    def analyzeAll(): Unit = {
      val encConn = DriverManager.getConnection(encDbUrl, encDbProps)
      try {
        encConn.prepareStatement("analyze").execute()
      } finally {
        encConn.close()
      }
    }

    dexStandaloneVariant match {
      case DexSpx =>
        buildEncRidTables()
        buildTFilter()
        buildTUncorrelatedJoin()
        analyzeAll()
      case DexCorr =>
        buildEncRidTables()
        buildTFilter()
        buildTCorrelatedJoin()
        analyzeAll()
      case x => throw DexException("unsupported: " + x.getClass.toString)
    }
  }

  private def encryptTable(table: TableName, ridDf: DataFrame): (String, DataFrame) = {
    require(ridDf.columns.contains("rid"))
    val colToRandCol = ridDf.columns.collect {
      case c if c == "rid" => "rid" -> "rid"
      case c => c -> dexColNameOf(c)
    }.toMap
    val ridDfProject = colToRandCol.values.map(col).toSeq

    (dexTableNameOf(table),
      ridDf.columns.foldLeft(ridDf) {
        case (d, c) if c == "rid" =>
          d.withColumn(colToRandCol(c), udfRid(col(c)))
        case (d, c) =>
          d.withColumn(colToRandCol(c), udfCell(col(c)))
      }.select(ridDfProject: _*))
  }
}
