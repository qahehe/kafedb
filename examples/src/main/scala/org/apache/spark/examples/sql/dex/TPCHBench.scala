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

import org.apache.spark.examples.sql.dex.TPCHDataGen.time
import org.apache.spark.sql.{DataFrame, SparkSession}
// scalastyle:off

object TPCHBench {

  def main(args: Array[String]): Unit = {
    require(args.length == 2)
    val emmMode = args(0)
    require(TPCHDataGen.emmModes.contains(emmMode))
    val translationMode = args(1)
    println(s"translationMode=$translationMode")

    SparkSession.cleanupAnyExistingSession()
    val spark = SparkSession
      .builder()
      .appName("TPCH Bench")
      .config("spark.sql.dex.translationMode", translationMode)
      .getOrCreate()
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
    println(s"spark.sql.dex.translationMode=${spark.conf.get("spark.sql.dex.translationMode")}")

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

    def benchQuery(spark: SparkSession, query: String, queryDf: DataFrame, queryDex: Option[DataFrame] = None): Unit = {
      println(s"\nBench query=\n$query")
      time {
        //val sparkResult = queryDf.count()
        //println(s"spark result size=${sparkResult}")
      }
      time {
        val postgresResult = spark.read.jdbc(TPCHDataGen.dbUrl, s"($query) as postgresResult", TPCHDataGen.dbProps)
        println(s"postgres result size=${postgresResult.count()}")
      }
      time {
        val dexResult = queryDex.getOrElse(emmMode match {
          case "standalone" => queryDf.dex(cks)
          case "pkfk" => queryDf.dexPkFk(pks, fks)
        })
        println(s"dex result size=${dexResult.count()}")
      }
    }

    println(s"\n Q2")
    // TPCH Query 2
    val q2a = "select r_comment from region where r_name = 'EUROPE'"
    val q2aDf = region.where("r_name == 'EUROPE'")
    benchQuery(spark, q2a, q2aDf)

    val q2b = "select n_name from region, nation where r_regionkey = n_regionkey"
    val q2bDf = region.join(nation).where("r_regionkey = n_regionkey").select("n_name")
    benchQuery(spark, q2b, q2bDf)

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
    //val q2cMain = region.where("r_name == 'EUROPE'")
    //  .join(nation).where("r_regionkey == n_regionkey")
    //  .join(supplier).where("n_nationkey == s_nationkey")
    //  .join(partsupp).where("s_suppkey == ps_suppkey")
    //  .join(part).where("ps_partkey == p_partkey and p_size == 15")

    val q2cDf = q2cMain
    val q2cDex = q2cMain.dexPkFk(pks, fks)
    benchQuery(spark, q2c, q2cDf)

    val q2d = "select * from part where p_size = 15"
    val q2dDf = part.where("p_size == 15")
    benchQuery(spark, q2d, q2dDf)

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
    //val q2eMain = part.join(partsupp).where("p_partkey == ps_partkey")
    val q2eMain = partsupp.join(part).where("ps_partkey == p_partkey")
        //.where("p_size == 15")
        .select("ps_supplycost")
    val q2eDf = q2eMain
    //val q2eDex = q2eMain.dexPkFk(pks, fks)
    benchQuery(spark, q2e, q2eDf)

    /*println("\n Q3")
    val q3a =
      """
        |select
        |  l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
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
      .join(lineitem).where("l_orderkey == o_orderkey")
      .select("l_orderkey", "l_extendedprice", "l_discount", "o_orderdate", "o_shippriority")
    val q3aDex = q3aDf.dex
    benchQuery(spark, q3a, q3aDf)*/

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
    //val q5aDex = q5aDf.dexPkFk(pks, fks)
    benchQuery(spark, q5a, q5aDf)

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
    //val q5bDex = q5bDf.dexPkFk(pks, fks)
    benchQuery(spark, q5b, q5bDf)

    /*val q5a = "select * from customer, supplier, nation where c_nationkey = n_nationkey and s_nationkey = n_nationkey"
    val q5aDf = customer.join(nation).join(supplier).where("c_nationkey == s_nationkey and n_nationkey == s_nationkey")
    val q5aDex = q5aDf.dex
    benchQuery(spark, q5a, q5aDf, q5aDex)

    val q5b = "select * from customer, supplier, nation, region where c_nationkey = s_nationkey and c_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' limit 100"
    val q5bDf = customer.join(supplier).where("c_nationkey = s_nationkey")
      .join(nation).where("c_nationkey = n_nationkey")
      .join(region).where("n_regionkey = r_regionkey and r_name == 'ASIA'")
    val q5bDex = q5bDf.dex.limit(100)
    benchQuery(spark, q5b, q5bDf, q5bDex)

    val q5c =
      """
        |select
        |  n_name,
        |  l_extendedprice,
        |  l_discount
        |from
        |  customer,
        |  orders,
        |  lineitem,
        |  supplier,
        |  nation,
        |  region
        |where
        |  c_custkey = o_custkey
        |  and l_orderkey = o_orderkey
        |  and l_suppkey = s_suppkey
        |  and c_nationkey = s_nationkey
        |  and s_nationkey = n_nationkey
        |  and n_regionkey = r_regionkey
        |  and r_name = 'ASIA'
      """.stripMargin
    val q5cDf = customer.join(orders).where("c_custkey == o_custkey")
        .join(lineitem).where("l_orderkey == o_orderkey")
        .join(supplier).where("l_suppkey == s_suppkey and c_nationkey == s_nationkey")
        .join(nation).where("n_nationkey == s_nationkey and c_nationkey == s_nationkey")
        .join(region).where("n_regionkey == r_regionkey and r_name == 'ASIA'")
        .select("n_name", "l_extendedprice", "l_discount")
    val q5cDex = q5cDf.dex
    benchQuery(spark, q5c, q5cDf, q5cDex)

    println("\n Q7")
    val q7a =
      """
        |select
        |  n1.n_name as supp_nation,
        |  n2.n_name as cust_nation,
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
        |    (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
        |  )
      """.stripMargin
    val q7aDf = supplier.join(lineitem).where("l_suppkey == s_suppkey")
        .join(orders).where("l_orderkey == o_orderkey")
        .join(customer).where("c_custkey == o_custkey")
        .join(nation.as("n1")).where("n1.n_nationkey == s_nationkey")
        .join(nation.as("n2")).where("c_nationkey == n2.n_nationkey")
        .where("n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'")
        .selectExpr("n1.n_name as supp_nation", "n2.n_name as cust_nation", "l_shipdate", "l_extendedprice", "l_discount")
    val q7aDex = q7aDf.dex
    benchQuery(spark, q7a, q7aDf, q7aDex)*/


    spark.stop()
  }
}
