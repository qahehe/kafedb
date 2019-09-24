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
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    def benchQuery(title: String, spark: SparkSession, query: String, queryDf: DataFrame, queryDex: Option[DataFrame] = None): Unit = {
      println(s"\n$title=\n$query")
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
        println(s"dex-$emmMode result size=${dexResult.count()}")
      }
    }

    println(s"\n Q2")
    // TPCH Query 2
    val q2a = "select r_comment from region where r_name = 'EUROPE'"
    val q2aDf = region.where("r_name == 'EUROPE'")
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
    val q2cDf = q2cMain
    benchQuery("q2c", spark, q2c, q2cDf)

    val q2c2Df = partsupp.join(part).where("ps_partkey == p_partkey and p_size == 15")
      .join(supplier).where("ps_suppkey == s_suppkey")
      .join(nation).where("s_nationkey == n_nationkey")
      .join(region.where("r_name == 'EUROPE'")).where("n_regionkey == r_regionkey")
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
    benchQuery("q5a", spark, q5a, q5aDf)

    val q5a2Df = supplier.join(
      customer.join(nation).where("c_nationkey == n_nationkey")
        .join(region).where("n_regionkey == r_regionkey and r_name == 'ASIA'")
    ).where("s_nationkey == n_nationkey")
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
    //val q5bDex = q5bDf.dexPkFk(pks, fks)
    benchQuery("q5b", spark, q5b, q5bDf)

    val q5b2Df =
      supplier.join(
        customer.join(nation).where("c_nationkey == n_nationkey")
      ).where("s_nationkey == n_nationkey")
        .join(region).where("n_regionkey == r_regionkey")
    benchQuery("q5b2", spark, q5b, q5b2Df)

    spark.stop()
  }
}
