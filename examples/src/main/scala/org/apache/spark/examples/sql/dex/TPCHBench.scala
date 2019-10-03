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
import org.apache.spark.sql.dex.{DexCorr, DexPkFk, DexSpx, DexVariant}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TPCHBench {

  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    val dexVariant = DexVariant.from(args(0))

    SparkSession.cleanupAnyExistingSession()
    val spark = SparkSession
      .builder()
      .appName("TPCH Bench")
      .getOrCreate()
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)

    TPCHDataGen.setScaleConfig(spark, TPCHDataGen.scaleFactor)

    val (dbname, tables, location) = TPCHDataGen.getBenchmarkData(spark, TPCHDataGen.scaleFactor)
    TPCHDataGen.pointDataToSpark(spark, dbname, tables, location)
    //tables.analyzeTables(dbname, analyzeColumns = true)

    val nameToDfForDex = TPCHDataGen.tableNamesToDex.map { t =>
      t -> spark.table(t)
    }.toMap

    val pks = TPCHDataGen.primaryKeys.map(_.attr.attr)
    val fks = TPCHDataGen.foreignKeys.map(_.attr.attr)
    val cks = TPCHDataGen.compoundKeys.map(_.attr)

    val part = nameToDfForDex("part")
    val partsupp = nameToDfForDex("partsupp")
    val supplier = nameToDfForDex("supplier")
    val nation = nameToDfForDex("nation")
    val region = nameToDfForDex("region")
    val customer = nameToDfForDex("customer")
    val orders = nameToDfForDex("orders")
    val lineitem = nameToDfForDex("lineitem")

    def benchQuery(title: String, spark: SparkSession, query: String, queryDf: DataFrame, queryDex: Option[DataFrame] = None): Unit = {
      println(s"\n$title=\n$query")
      /*time {
        val sparkResult = queryDf.count()
        println(s"spark result size=${sparkResult}")
      }*/
      time {
        val postgresResult = spark.read.jdbc(TPCHDataGen.dbUrl, s"($query) as postgresResult", TPCHDataGen.dbProps)
        println(s"postgres result size=${postgresResult.count()}")
      }
      time {
        val dexResult = queryDex.getOrElse(dexVariant match {
          case DexSpx => queryDf.dexSpx(cks)
          case DexCorr => queryDf.dexCorr(cks)
          case DexPkFk  => queryDf.dexPkFk(pks, fks)
        })
        println(s"dex-${dexVariant.getClass.toString} result size=${dexResult.count()}")
      }
    }

    println(s"\n Q2")
    // TPCH Query 2
    val q2a = "select r_comment from region where r_name = 'EUROPE'"
    val q2aDf = region.where("r_name == 'EUROPE'").select("r_comment")
    benchQuery("q2a", spark, q2a, q2aDf)

    val q2b = "select n_name from region, nation where r_regionkey = n_regionkey"
    val q2bDf = region.join(nation).where("r_regionkey = n_regionkey").select("n_name")
    benchQuery("q2b", spark, q2b, q2bDf)

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
    val q2cMain = part.where("p_size == 15")
      .join(partsupp).where("p_partkey == ps_partkey")
      .join(supplier).where("ps_suppkey == s_suppkey")
      .join(nation).where("s_nationkey == n_nationkey")
      .join(region.where("r_name == 'EUROPE'")).where("n_regionkey == r_regionkey")
      .select("ps_supplycost")
    val q2cDf = q2cMain
    benchQuery("q2c", spark, q2c, q2cDf)

    val q2c2Df = partsupp.join(part).where("ps_partkey == p_partkey and p_size == 15")
      .join(supplier).where("ps_suppkey == s_suppkey")
      .join(nation).where("s_nationkey == n_nationkey")
      .join(region.where("r_name == 'EUROPE'")).where("n_regionkey == r_regionkey")
      .select("ps_supplycost")
    benchQuery("q2c2", spark, q2c, q2c2Df)

    val q2d = "select * from part where p_size = 15"
    val q2dDf = part.where("p_size == 15")
    benchQuery("q2d", spark, q2d, q2dDf)

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
    benchQuery("q2e", spark, q2e, q2eDf)

    val q2e2Df = part.join(partsupp).where("p_partkey == ps_partkey")
      .select("ps_supplycost")
    benchQuery("q2e2", spark, q2e, q2e2Df)

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
    benchQuery("q3a", spark, q3, q3aDf)*/

    println("\n Q5")
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
    benchQuery("q5a", spark, q5a, q5aDf)

    val q5a2Df = supplier.join(
      customer.join(nation).where("c_nationkey == n_nationkey")
        .join(region).where("n_regionkey == r_regionkey and r_name == 'ASIA'")
    ).where("s_nationkey == n_nationkey")
        .select("n_name")
    benchQuery("q5a2", spark, q5a, q5a2Df)

    val q5b =
      """
        |select
        |  n_name
        |from
        |  customer, supplier, nation, region
        |where
        |  r_regionkey = n_regionkey
        |  and n_nationkey = c_nationkey
        |  and n_nationkey = s_nationkey
      """.stripMargin
    val q5bDf = region
      .join(nation).where("r_regionkey == n_regionkey")
      .join(customer).where("n_nationkey == c_nationkey")
      .join(supplier).where("n_nationkey == s_nationkey")
        .select("n_name")
    //val q5bDex = q5bDf.dexPkFk(pks, fks)
    benchQuery("q5b", spark, q5b, q5bDf)

    val q5b2Df =
      supplier.join(
        customer.join(nation).where("c_nationkey == n_nationkey")
      ).where("s_nationkey == n_nationkey")
        .join(region).where("n_regionkey == r_regionkey")
    benchQuery("q5b2", spark, q5b, q5b2Df)

    // Q6 has only range queries, skip.

    println("\nQ7")
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
        |    or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
        |  )
      """.stripMargin
    val q7aDf = nation.as("n1").join(supplier).where("n1.n_nationkey == s_nationkey")
        .join(
          nation.as("n2").join(customer).where("n2.n_nationkey == c_nationkey")
            .join(orders).where("c_custkey == o_custkey")
            .join(lineitem).where("o_orderkey == l_orderkey")
        )
        .where("s_suppkey = l_suppkey")
        .selectExpr("n1.n_name as n1_name", "n2.n_name as n2_name", "l_shipdate", "l_extendedprice", "l_discount")
    benchQuery("q7a", spark, q7, q7aDf)


    println("\nQ8")
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
    val q8aDf = region.where("r_name == 'AMERICA'")
        .join(nation.as("n1")).where("r_regionkey == n1.n_regionkey")
        .join(customer).where("n1.n_nationkey == c_nationkey")
        .join(orders).where("c_custkey == o_custkey")
        .join(
          part.where("p_type == 'ECONOMY ANODIZED STEEL'")
            .join(lineitem).where("p_partkey = l_partkey")
            .join(supplier).where("s_suppkey = l_suppkey")
            .join(nation.as("n2")).where("s_nationkey = n2.n_nationkey")
        )
        .selectExpr("o_orderdate", "l_extendedprice", "l_discount", "n2.n_name")
    benchQuery("q8a", spark, q8, q8aDf)

    println("\n Q9")
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
    val q9aDf = lineitem.join(
      partsupp.join(part).where("ps_partkey = p_partkey")
        .join(supplier).where("ps_suppkey = s_suppkey")
    ).where("l_partkey = ps_partkey and l_suppkey = ps_suppkey")
      .select("n_name", "o_orderdate", "l_extendedprice", "l_discount", "ps_supplycost", "l_quantity")
    benchQuery("q9a", spark, q9, q9aDf)

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
    val q10aDf = lineitem.where("l_returnflag = 'R'")
        .join(orders).where("l_orderkey = o_orderkey")
        .join(customer).where("o_custkey = c_custkey")
        .join(nation).where("c_custkey = n_nationkey")
        .select("c_name", "l_extendedprice", "l_discount", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
    benchQuery("q10a", spark, q10, q10aDf)


    spark.stop()
  }
}
