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
import org.apache.spark.sql.functions._
// scalastyle:off

object TPCHBench {

  def main(args: Array[String]): Unit = {
    val spark = TPCHDataGen.newSparkSession()

    val nameToDfForDex = TPCHDataGen.tableNamesToDex.map { t =>
      t -> spark.table(t)
    }.toMap

    println(s"\n benchmark 1")
    // TPCH Query 2
    time {
      val q2a = nameToDfForDex("region").where("r_name == 'EUROPE'").dex.collect()
      println(s"q2a count=${q2a.length}")
    }
    time {
      val q2b = nameToDfForDex("nation").join(nameToDfForDex("region")).where("n_regionkey = r_regionkey").select("n_name").dex.collect()
      println(s"q2b count=${q2b.length}")
    }

    time {
      // 		select
      //			min(ps_supplycost)
      //		from
      //      part,
      //			partsupp,
      //			supplier,
      //			nation,
      //			region
      //		where
      //			p_partkey = ps_partkey
      //			and s_suppkey = ps_suppkey
      //			and s_nationkey = n_nationkey
      //			and n_regionkey = r_regionkey
      //			and r_name = 'EUROPE'
      val part = nameToDfForDex("part")
      val partsupp = nameToDfForDex("partsupp")
      val supplier = nameToDfForDex("supplier")
      val nation = nameToDfForDex("nation")
      val region = nameToDfForDex("region")

      val q2c = part.join(partsupp).where("p_partkey == ps_partkey")
        .join(supplier).where("ps_suppkey == s_suppkey")
        .join(nation).where("s_nationkey== n_nationkey")
        .join(region).where("n_regionkey = r_regionkey")
        .where("p_size = 15 AND r_name == 'EUROPE'")
        .select("ps_supplycost")
        .dex
        .agg(min("ps_supplycost"))
        .collect()

      println(s"q2c count=${q2c.length}")
    }
  }
}
