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

import org.apache.spark.examples.sql.dex.TPCHDataGen.time
import org.apache.spark.sql.dex.DexConstants.AttrName
import org.apache.spark.sql.dex.{DexCorr, DexPkFk, DexSpx, DexVariant}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BenchVariant {
  def from(name: String): BenchVariant = {
    name.toLowerCase() match {
      case "spark" => Spark
      case "postgres" => Postgres
      case x if x.contains("dex") => Dex(DexVariant.from(x))
    }
  }
}
sealed trait BenchVariant {
  def name: String = getClass.getSimpleName
}
case object Spark extends BenchVariant
case object Postgres extends BenchVariant
case class Dex(variant: DexVariant) extends BenchVariant {
  override def name: String = variant.name
}

trait DexTPCHBenchCommon {
  SparkSession.cleanupAnyExistingSession()
  lazy val spark: SparkSession = {
    val s = SparkSession
      .builder()
      .appName("TPCH Bench")
      .getOrCreate()
    SparkSession.setActiveSession(s)
    SparkSession.setDefaultSession(s)

    TPCHDataGen.setScaleConfig(s, TPCHDataGen.scaleFactor)

    val (dbname, tables, location) = TPCHDataGen.getBenchmarkData(s, TPCHDataGen.scaleFactor)
    TPCHDataGen.pointDataToSpark(s, dbname, tables, location)
    //tables.analyzeTables(dbname, analyzeColumns = true)
    s
  }

  lazy val nameToDfForDex: Map[String, DataFrame] = TPCHDataGen.tableNamesToDex.map { t =>
    t -> spark.table(t)
  }.toMap

  lazy val pks: Set[AttrName] = TPCHDataGen.primaryKeys.map(_.attr.attr)
  lazy val fks: Set[AttrName] = TPCHDataGen.foreignKeys.map(_.attr.attr)
  lazy val cks: Set[AttrName] = TPCHDataGen.compoundKeys.map(_.attr)

  lazy val part: DataFrame = nameToDfForDex("part")
  lazy val partsupp: DataFrame = nameToDfForDex("partsupp")
  lazy val supplier: DataFrame = nameToDfForDex("supplier")
  lazy val nation: DataFrame = nameToDfForDex("nation")
  lazy val region: DataFrame = nameToDfForDex("region")
  lazy val customer: DataFrame = nameToDfForDex("customer")
  lazy val orders: DataFrame = nameToDfForDex("orders")
  lazy val lineitem: DataFrame = nameToDfForDex("lineitem")

  def benchQuery(variant: BenchVariant, title: String, query: String, queryDf: DataFrame, queryDex: Option[DataFrame] = None): Unit = {
    println(s"\n$title=\n$query")
    time {
      val result = variant match {
        case Spark => queryDf
        case Postgres => spark.read.jdbc(TPCHDataGen.dbUrl, s"($query) as postgresResult", TPCHDataGen.dbProps)
        case d: Dex => queryDex.getOrElse(d.variant match {
          case DexSpx => queryDf.dexSpx(cks)
          case DexCorr => queryDf.dexCorr(cks)
          case DexPkFk  => queryDf.dexPkFk(pks, fks)
        })
      }
      println(s"${variant.name} result size=${result.count()}")
    }
  }

  def benchQuery(variant: BenchVariant, bq: BenchQuery): BenchQueryResult = {
    println(s"\n${bq.name}=\n${bq.query}")
    val (resultCount, duration) = time {
      val result = variant match {
        case Spark => bq.queryDf
        case Postgres => spark.read.jdbc(TPCHDataGen.dbUrl, s"(${bq.query}) as postgresResult", TPCHDataGen.dbProps)
        case d: Dex => d.variant match {
          case DexSpx => bq.queryDf.dexSpx(cks)
          case DexCorr => bq.queryDf.dexCorr(cks)
          case DexPkFk  => bq.queryDf.dexPkFk(pks, fks)
        }
      }
      val c = result.count()
      println(s"${variant.name} result size=$c")
      c
    }
    BenchQueryResult(bq.name, resultCount, duration)
  }
}

object TPCHBenchOld extends DexTPCHBenchCommon {

  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    val variant = BenchVariant.from(args(0))

    println(s"\n Q2")
    val q2a = "select r_comment from region where r_name = 'EUROPE'"
    val q2aDf = region.where("r_name == 'EUROPE'").select("r_comment")
    benchQuery(variant, "q2a", q2a, q2aDf)

    val q2b = "select n_name from region, nation where r_regionkey = n_regionkey"
    val q2bDf = region.join(nation).where("r_regionkey = n_regionkey").select("n_name")
    benchQuery(variant, "q2b", q2b, q2bDf)

    // flat:
    //      P
    //    /
    // PS     N  - R
    //    \   /
    //      S
    val q2c =
      """
        |select
        |  ps_supplycost
        |from
        |  part,
        |  partsupp,
        |  supplier,
        |  nation,
        |  region
        |where
        |  p_partkey = ps_partkey
        |  and ps_suppkey = s_suppkey
        |  and s_nationkey = n_nationkey
        |  and n_regionkey = r_regionkey
        |  and r_name = 'EUROPE'
        |  and p_size = 15
      """.stripMargin
    // filter on P first, R last
    val q2cMain = part.where("p_size == 15")
      .join(partsupp).where("p_partkey == ps_partkey")
      .join(supplier).where("ps_suppkey == s_suppkey")
      .join(nation).where("s_nationkey == n_nationkey")
      .join(region.where("r_name == 'EUROPE'")).where("n_regionkey == r_regionkey")
      .select("ps_supplycost")
    val q2cDf = q2cMain
    benchQuery(variant, "q2c", q2c, q2cDf)

    // filter on P later than join, R last
    val q2c2Df = partsupp.join(part).where("ps_partkey == p_partkey and p_size == 15")
      .join(supplier).where("ps_suppkey == s_suppkey")
      .join(nation).where("s_nationkey == n_nationkey")
      .join(region.where("r_name == 'EUROPE'")).where("n_regionkey == r_regionkey")
      .select("ps_supplycost")
    benchQuery(variant, "q2c2", q2c, q2c2Df)

    // filter on R first, P last
    val q2c3Df = region.where("r_name == 'EUROPE'")
      .join(nation).where("r_regionkey = n_nationkey")
      .join(supplier).where("n_nationkey = s_nationkey")
      .join(partsupp).where("s_suppkey = ps_suppkey")
      .join(part).where("ps_partkey = p_partkey and p_size = 15")
      .select("ps_supplycost")
    benchQuery(variant, "q2c3", q2c, q2c3Df)

    // frilter on R first, P beyond join
    val q2c4Df = region.where("r_name == 'EUROPE'")
      .join(nation).where("r_regionkey = n_nationkey")
      .join(supplier).where("n_nationkey = s_nationkey")
      .join(part
          .join(partsupp).where("p_size = 15 and p_partkey = ps_partkey")
      ).where("s_suppkey = ps_suppkey")
      .select("ps_supplycost")
    benchQuery(variant, "q2c4", q2c, q2c4Df)


    val q2d = "select * from part where p_size = 15"
    val q2dDf = part.where("p_size == 15")
    benchQuery(variant, "q2d", q2d, q2dDf)

    val q2e =
      """
        |select
        |  ps_supplycost
        |from
        |  part,
        |  partsupp
        |where
        |  p_partkey = ps_partkey
      """.stripMargin
    val q2eMain = partsupp.join(part).where("ps_partkey == p_partkey")
        .select("ps_supplycost")
    val q2eDf = q2eMain
    //val q2eDex = q2eMain.dexPkFk(pks, fks)
    benchQuery(variant, "q2e", q2e, q2eDf)

    val q2e2Df = part.join(partsupp).where("p_partkey == ps_partkey")
      .select("ps_supplycost")
    benchQuery(variant, "q2e2", q2e, q2e2Df)

    /*println("\n Q3")
    val q3 =
      """
        |select
        |  l_extendedprice,
        |  l_discount,
        |  o_orderdate,
        |  o_shippriority
        |from
        |  customer,
        |  orders,
        |  lineitem
        |where
        |  c_mktsegment = 'BUILDING'
        |  and c_custkey = o_custkey
        |  and l_orderkey = o_orderkey
      """.stripMargin
    val q3aDf = customer.where("c_mktsegment == 'BUILDING'")
        .join(orders).where("c_custkey == o_custkey")
        .join(lineitem).where("o_orderkey == l_orderkey")
    benchQuery(variant, "q3a", spark, q3, q3aDf)*/

    println("\n Q5")
    // Triangle
    //  C  -  S
    //   \   /
    //     N  -  R
    val q5a =
      """
        |select
        |  n_name
        |from
        |  customer, supplier, nation, region
        |where
        |  r_name = 'ASIA'
        |  and r_regionkey = n_regionkey
        |  and n_nationkey = c_nationkey
        |  and n_nationkey = s_nationkey
      """.stripMargin
    val q5aDf = region.where("r_name == 'ASIA'")
        .join(nation).where("r_regionkey == n_regionkey")
        .join(customer).where("n_nationkey == c_nationkey")
        .join(supplier).where("n_nationkey == s_nationkey")
        .select("n_name")
    //val q5aDex = q5aDf.dexPkFk(pks, fks)
    benchQuery(variant, "q5a", q5a, q5aDf)

    val q5a2Df = supplier.join(
      customer
        .join(nation).where("c_nationkey == n_nationkey")
        .join(region).where("n_regionkey == r_regionkey and r_name == 'ASIA'")
    ).where("s_nationkey == n_nationkey")
        .select("n_name")
    benchQuery(variant, "q5a2", q5a, q5a2Df)

    // same as q5a without filter
    val q5b =
      """
        |select
        |  n_name
        |from
        |  customer, supplier, nation, region
        |where
        |  r_regionkey = n_regionkey
        |  and c_nationkey = s_nationkey
        |  and n_nationkey = c_nationkey
        |  and n_nationkey = s_nationkey
      """.stripMargin
    val q5bDf = region
      .join(nation).where("r_regionkey == n_regionkey")
      .join(customer).where("n_nationkey == c_nationkey")
      .join(supplier).where("n_nationkey == s_nationkey")
        .select("n_name")
    benchQuery(variant, "q5b", q5b, q5bDf)

    val q5b2Df =
      supplier.join(customer
          .join(nation).where("c_nationkey == n_nationkey")
          .join(region).where("n_regionkey == r_regionkey")
      ).where("s_nationkey == n_nationkey")
        .select("n_name")
    benchQuery(variant, "q5b2", q5b, q5b2Df)

    // Q6 has only range queries, skip.

    println("\nQ7")
    //  N1 - S - L - O - C - N2
    val q7 =
      """
        |select
        |  n1.n_name as n1_name,
        |  n2.n_name as n2_name,
        |  l_shipdate,
        |  l_extendedprice,
        |  l_discount
        |from
        |  supplier,
        |  lineitem,
        |  orders,
        |  customer,
        |  nation n1,
        |  nation n2
        |where
        |  s_suppkey = l_suppkey
        |  and o_orderkey = l_orderkey
        |  and c_custkey = o_custkey
        |  and s_nationkey = n1.n_nationkey
        |  and c_nationkey = n2.n_nationkey
        |  and (
        |    (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
        |  )
      """.stripMargin
    val q7aDf = customer.join(
      orders.join(
        supplier.join(lineitem).where("s_suppkey = l_suppkey")
          .join(nation.as("n1")).where("s_nationkey = n1.n_nationkey and n1.n_name = 'FRANCE'")
      ).where("o_orderkey = l_orderkey")
    ).where("c_custkey = o_custkey")
      .join(nation.as("n2")).where("c_nationkey = n2.n_nationkey and n2.n_name = 'GERMANY'")
      .selectExpr("n1.n_name as n1_name", "n2.n_name as n2_name", "l_shipdate", "l_extendedprice", "l_discount")
    benchQuery(variant, "q7a", q7, q7aDf)

    val q7bDf = nation.as("n1").where("n1.n_name = 'FRANCE'")
      .join(supplier).where("n1.n_nationkey = s_nationkey")
      .join(lineitem).where("s_suppkey = l_suppkey")
      .join(orders).where("l_orderkey = o_orderkey")
      .join(customer).where("o_custkey = c_custkey")
      .join(nation.as("n2")).where("c_nationkey = n2.n_nationkey and n2.n_name = 'GERMANY'")
      .selectExpr("n1.n_name as n1_name", "n2.n_name as n2_name", "l_shipdate", "l_extendedprice", "l_discount")
    benchQuery(variant, "q7b", q7, q7bDf)

    val q7cDf = nation.as("n2").where("n2.n_name = 'GERMANY'")
      .join(customer).where("n2.n_nationkey = c_nationkey")
      .join(orders).where("c_custkey = o_custkey")
      .join(lineitem).where("o_orderkey = l_orderkey")
      .join(supplier).where("l_suppkey = s_suppkey")
      .join(nation.as("n1")).where("s_nationkey = n1.n_nationkey and n1.n_name = 'FRANCE'")
      .selectExpr("n1.n_name as n1_name", "n2.n_name as n2_name", "l_shipdate", "l_extendedprice", "l_discount")
    benchQuery(variant, "q7c", q7, q7cDf)


    println("\nQ8")
    // q7c plus filter on dimension table P and subtract filter on N2
    // snowflake
    //           f(P)
    //           |
    //  N2 - S - L - O - C - N1 - f(R)
    val q8 =
      """
        |select
        |  o_orderdate,
        |  l_extendedprice,
        |  l_discount,
        |  n2.n_name
        |from
        |  part,
        |  supplier,
        |  lineitem,
        |  orders,
        |  customer,
        |  nation n1,
        |  nation n2,
        |  region
        |where
        |  p_partkey = l_partkey
        |  and s_suppkey = l_suppkey
        |  and l_orderkey = o_orderkey
        |  and o_custkey = c_custkey
        |  and c_nationkey = n1.n_nationkey
        |  and n1.n_regionkey = r_regionkey
        |  and r_name = 'AMERICA'
        |  and s_nationkey = n2.n_nationkey
        |  and p_type = 'ECONOMY ANODIZED STEEL'
      """.stripMargin
    // as SQL
    val q8aDf = supplier.join(
      part.join(lineitem).where("p_type = 'ECONOMY ANODIZED STEEL' and p_partkey = l_partkey")
        .join(orders).where("l_orderkey = o_orderkey")
        .join(customer).where("o_custkey = c_custkey")
        .join(nation.as("n1")).where("c_nationkey = n1.n_nationkey")
        .join(region).where("r_name = 'AMERICA' and n1.n_regionkey = r_regionkey")
    ).where("s_suppkey = l_suppkey")
        .join(nation.as("n2")).where("s_nationkey = n2.n_nationkey")
      .selectExpr("o_orderdate", "l_extendedprice", "l_discount", "n2.n_name")
    benchQuery(variant, "q8a", q8, q8aDf)

    // filters on P and R first, filter on R as right subtree, large intermediate data from L - O subtrees-join, need indices on both subtrees
    val q8a2Df = part.where("p_type = 'ECONOMY ANODIZED STEEL'")
        .join(lineitem).where("p_partkey = l_partkey")
        .join(
          region.where("r_name = 'AMERICA'")
          .join(nation.as("n1")).where("r_regionkey = n1.n_regionkey")
            .join(customer).where("n1.n_nationkey = c_nationkey")
            .join(orders).where("c_custkey = o_custkey")
        ).where("l_orderkey = o_orderkey")
        .join(supplier).where("l_suppkey = s_suppkey")
        .join(nation.as("n2")).where("s_nationkey = n2.n_nationkey")
      .selectExpr("o_orderdate", "l_extendedprice", "l_discount", "n2.n_name")
    benchQuery(variant, "q8a2", q8, q8a2Df)

    // same as q8a2, but filter on P is right subtree
    val q8a3Df = region.where("r_name = 'AMERICA'")
        .join(nation.as("n1")).where("r_regionkey = n1.n_regionkey")
        .join(customer).where("n_nationkey = c_nationkey")
        .join(orders).where("c_custkey = o_custkey")
        .join(part
            .join(lineitem).where("p_type = 'ECONOMY ANODIZED STEEL' and p_partkey = l_partkey")
        ).where("o_orderkey = l_orderkey")
        .join(supplier).where("l_suppkey = s_suppkey")
        .join(nation.as("n2")).where("s_nationkey = n2.n_nationkey")
      .selectExpr("o_orderdate", "l_extendedprice", "l_discount", "n2.n_name")
    benchQuery(variant, "q8a3", q8, q8a3Df)

    println("\n Q9")
    // snowflake
    // N - S   PS  P
    //      \  |  /
    //         L
    //        /
    //       O
    val q9 =
      """
        |select
        |  n_name,
        |  o_orderdate,
        |  l_extendedprice,
        |  l_discount,
        |  ps_supplycost,
        |  l_quantity
        |from
        |  part, supplier, lineitem, partsupp, orders, nation
        |where
        |  s_suppkey = l_suppkey
        |  and ps_suppkey = l_suppkey and ps_partkey = l_partkey
        |  and p_partkey = l_partkey
        |  and o_orderkey = l_orderkey and s_nationkey = n_nationkey
      """.stripMargin
    // join on smaller tables
    val q9aDf = orders.join(
      part.join(
        partsupp.join(
          supplier
            .join(lineitem).where("s_suppkey = l_suppkey")
            .join(nation).where("s_nationkey = n_nationkey") // todo: optimization: join small table first
        ).where("ps_partkey = l_partkey and ps_suppkey = l_suppkey")
      ).where("p_partkey = l_partkey")
    ).where("o_orderkey = l_orderkey")
      .select("n_name", "o_orderdate", "l_extendedprice", "l_discount", "ps_supplycost", "l_quantity")
    benchQuery(variant, "q9a", q9, q9aDf)

    // order fk-pk join tables from large to small
    val q9bDf = lineitem.join(supplier).where("l_suppkey = s_suppkey")
      .join(nation).where("s_nationkey = n_nationkey")
      .join(part).where("l_partkey = p_partkey")
      .join(partsupp).where("l_partkey = ps_partkey and l_suppkey = ps_suppkey")
      .join(orders).where("l_orderkey = o_orderkey")
      .select("n_name", "o_orderdate", "l_extendedprice", "l_discount", "ps_supplycost", "l_quantity")
    benchQuery(variant, "q9b", q9, q9bDf)

    // order fk-pk join tables from large to small
    val q9cDf = lineitem
      .join(orders).where("l_orderkey = o_orderkey")
      .join(partsupp).where("l_partkey = ps_partkey and l_suppkey = ps_suppkey")
      .join(part).where("l_partkey = p_partkey")
      .join(supplier).where("l_suppkey = s_suppkey")
      .join(nation).where("s_nationkey = n_nationkey")

      .select("n_name", "o_orderdate", "l_extendedprice", "l_discount", "ps_supplycost", "l_quantity")
    benchQuery(variant, "q9c", q9, q9cDf)

    println("\n Q10")
    val q10 =
      """
        |select
        |  c_name,
        |  l_extendedprice,
        |  l_discount,
        |  c_acctbal,
        |  n_name,
        |  c_address,
        |  c_phone,
        |  c_comment
        |from
        |  customer,
        |  orders, lineitem, nation
        |where
        |  c_custkey = o_custkey
        |  and l_orderkey = o_orderkey
        |  and l_returnflag = 'R'
        |  and c_nationkey = n_nationkey
      """.stripMargin
    // filter on fk for fk-pk join
    /*val q10aDf = lineitem.where("l_returnflag = 'R'")
        .join(orders).where("l_orderkey = o_orderkey")
        .join(customer).where("o_custkey = c_custkey")
        .join(nation).where("c_custkey = n_nationkey")
        .select("c_name", "l_extendedprice", "l_discount", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")*/
    val q10aDf = lineitem.join(
      customer.join(orders).where("c_custkey = o_custkey")
        .join(nation).where("c_nationkey = n_nationkey")
    ).where("l_returnflag = 'R' and l_orderkey = o_orderkey")
      .select("c_name", "l_extendedprice", "l_discount", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
    benchQuery(variant, "q10a", q10, q10aDf)

    /*println("\nQ11")
    val q11 =
      """
        |select
        |  ps_partkey,
        |  ps_supplycost,
        |  ps_availqty
        |from
        |  partsupp, supplier, nation
        |where
        |  ps_suppkey = s_suppkey
        |  and s_nationkey = n_nationkey
        |  and n_name = 'GERMANY'
      """.stripMargin
    val q11aDf = partsupp.join(supplier).where("ps_suppkey = s_suppkey")
        .join(nation).where("s_nationkey = n_nationkey and n_name = 'GERMANY'")
        .select("ps_partkey", "ps_supplycost", "ps_availqty")
    benchQuery(variant, "q11a", spark, q11, q11aDf)

    println("\nQ12")
    val q12 =
      """
        |select
        |  l_shipmode,
        |  o_orderpriority
        |from
        |  orders,
        |  lineitem
        |where
        |  o_orderkey = l_orderkey
        |  and l_shipmode in ('MAIL', 'SHIP')
      """.stripMargin*/

    spark.stop()
  }
}
