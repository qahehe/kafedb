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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
// scalastyle:off

object TPCHBench {

  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    val translationMode = args(0)
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
    tables.analyzeTables(dbname, analyzeColumns = true)

    val nameToDfForDex = TPCHDataGen.tableNamesToDex.map { t =>
      t -> spark.table(t)
    }.toMap

    val part = nameToDfForDex("part")
    val partsupp = nameToDfForDex("partsupp")
    val supplier = nameToDfForDex("supplier")
    val nation = nameToDfForDex("nation")
    val region = nameToDfForDex("region")

    println(s"\n benchmark 1")
    // TPCH Query 2
    val q2a = "select * from region where r_name = 'EUROPE'"
    val q2aDf = region.where("r_name == 'EUROPE'")
    benchQuery(spark, q2a, q2aDf, q2aDf.dex)

    val q2b = "select n_name from nation, region where n_regionkey = r_regionkey"
    val q2bDf = nation.join(region).where("n_regionkey = r_regionkey").select("n_name")
    benchQuery(spark, q2b, q2bDf, q2bDf.dex)

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
    val q2cMain = part.join(partsupp).where("p_partkey == ps_partkey")
      .join(supplier).where("ps_suppkey == s_suppkey")
      .join(nation).where("n_nationkey== s_nationkey")
      .join(region).where("n_regionkey = r_regionkey")
      .where("r_name == 'EUROPE' and p_size == 15")
      .select("ps_supplycost")
    val q2cDf = q2cMain
    val q2cDex = q2cMain.dex
    benchQuery(spark, q2c, q2cDf, q2cDex)

    val q2d = "select * from part where p_size = 15"
    val q2dDf = part.where("p_size == 15")
    benchQuery(spark, q2d, q2dDf, q2dDf.dex)

    val q2e =
      """
        |select
        |  ps_supplycost
        |from
        |  part,
        |  partsupp
        |where
        |  p_partkey = ps_partkey
        |  and p_size = 15
      """.stripMargin
    val q2eMain = part.join(partsupp).where("p_partkey == ps_partkey")
        .where("p_size == 15")
        .select("ps_supplycost")
    val q2eDf = q2eMain
    val q2eDex = q2eMain.dex
    benchQuery(spark, q2e, q2eDf, q2eDex)

    spark.stop()
  }


  private def benchQuery(spark: SparkSession, query: String, queryDf: DataFrame, queryDex: DataFrame): Unit = {
    println(s"\nBench query=\n$query")
    time {
      val sparkResult = queryDf.collect()
      println(s"spark result size=${sparkResult.length}")
    }
    time {
      val postgresResult = spark.read.jdbc(TPCHDataGen.dbUrl, s"($query) as postgresResult", TPCHDataGen.dbProps).collect()
      println(s"postgres result size=${postgresResult.length}")
    }
    time {
      val dexResult = queryDex.collect()
      println(s"dexCorrelation result size=${dexResult.length}")
    }
  }
}
