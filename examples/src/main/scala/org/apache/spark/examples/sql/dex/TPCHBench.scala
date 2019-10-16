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

import org.apache.spark.sql.DataFrame

import scala.util.Random

object TPCHBench extends DexTPCHBenchCommon {

  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    val variant = BenchVariant.from(args(0))

    val queries = Seq(
      BenchQuery("q1",
        """
          |select
          |  l_returnflag,
          |  l_linestatus,
          |  sum(l_quantity) as sum_qty,
          |  sum(l_extendedprice) as sum_base_price,
          |  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
          |  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
          |  avg(l_quantity) as avg_qty,
          |  avg(l_extendedprice) as avg_price,
          |  avg(l_discount) as avg_disc,
          |  count(*) as count_order
          |from
          |  lineitem
        """.stripMargin,
        lineitem.selectExpr(
          "l_returnflag",
          "l_linestatus",
          "sum(l_quantity)",
          "sum(l_extendedprice)",
          "sum(l_extendedprice*(1-l_discount))",
          "sum(l_extendedprice*(1-l_discount)*(1+l_tax))",
          "avg(l_quantity)",
          "avg(l_extendedprice)",
          "avg(l_discount)",
          "count(*)")
      ),

      BenchQuery("q2",
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
          |  and s_suppkey = ps_suppkey
          |  and s_nationkey = n_nationkey
          |  and n_regionkey = r_regionkey
          |  and r_name = 'EUROPE'
          |  and p_size = 15
        """.stripMargin,
        region.where("r_name = 'EUROPE")
          .join(nation).where("r_regionkey = n_regionkey")
          .join(supplier).where("n_nationkey = s_nationkey")
          .join(part.where("p_size = 15")
            .join(partsupp).where("p_patkey = ps_partkey"))
          .where("s_suppkey = ps_suppkey")
          .select("ps_supplycost")
      ),

      BenchQuery("q3",
        """
          |select
          |  l_orderkey,
          |  l_extendedprice,
          |  l_discount
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
        """.stripMargin,
        lineitem
          .join(orders
            .join(customer).where("c_mktsegment = 'BUILDING' and c_custkey = o_custkey"))
          .where("l_orderkey = o_orderkey")
          .select("l_orderkey", "l_extendedprice", "l_discount", "o_orderdate", "o_shippriority")
      ),

      BenchQuery("q4",
        """
          |select
          |  *
          |from
          |  lineitem
          |where
          |  l_orderkey = o_orderkey
        """.stripMargin,
        lineitem.join(orders).where("l_orderkey = o_orderkey")
      ),

      //     L - O
      //    /     \
      //   S  ...  C
      //    \     /
      //       N
      //       |
      //      f(R)
      BenchQuery("q5b",
        """
          |select
          |  n_name,
          |  l_extendedprice,
          |  l_discount
          |from
          |  customer, orders, lineitem, supplier, nation, region
          |where
          |  c_custkey = o_custkey
          |  and l_orderkey = o_orderkey
          |  and l_suppkey = s_suppkey
          |  and c_nationkey = s_nationkey
          |  and s_nationkey = n_nationkey
          |  and n_regionkey = r_regionkey
          |  and r_name = 'ASIA'
        """.stripMargin,

        lineitem
          .join(orders).where("l_orderkey = o_orderkey")
          .join(customer).where("o_custkey = c_custkey")
          .join(region.where("r_name = 'ASIA'")
            .join(nation).where("r_regionkey = n_regionkey")
            .join(supplier).where("n_nationkey = s_natiokey"))
          .where("c_nationkey = n_nationkey")
      )

      //    L - O
      //   /     \
      //  S       C
      //  |       |
      // f(N1)  f(N2)
      /*BenchQuery("q6",
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
          |    (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
          |     or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
          |  )
        """.stripMargin,
      )*/
    )

    require(queries.map(_.name).toSet.size == queries.size, "unique query name")

    val benchResults = Random.shuffle(queries).map { q =>
      benchQuery(variant, q)
    }

    benchResults.sortBy(_.name).foreach { r =>
      println(s"query=${r.name}, count=${r.resultCount}, duration=${r.duration}")
    }

    spark.stop()
  }

}
