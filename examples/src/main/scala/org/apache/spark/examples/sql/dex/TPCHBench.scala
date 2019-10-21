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

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, expr}

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
          |from
          |  lineitem
        """.stripMargin,
        lineitem.selectExpr(
          "l_returnflag", "l_linestatus")
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
        region.where("r_name = 'EUROPE'")
          .join(nation).where("r_regionkey = n_regionkey")
          .join(supplier).where("n_nationkey = s_nationkey")
          .join(part.where("p_size = 15")
            .join(partsupp).where("p_partkey = ps_partkey"))
          .where("s_suppkey = ps_suppkey")
          .select("ps_supplycost")
      ),

      // f(C) - O - L
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
        customer.where("c_mktsegment = 'BUILDING'")
          .join(orders).where("c_custkey = o_custkey")
          .join(lineitem).where("o_orderkey = l_orderkey")
          .select("l_orderkey", "l_extendedprice", "l_discount", "o_orderdate", "o_shippriority")
      ),

      // L - O
      BenchQuery("q4",
        """
          |select
          |  *
          |from
          |  lineitem, orders
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
      BenchQuery("q5",
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

        region.where("r_name = 'ASIA'")
          .join(nation).where("r_regionkey = n_regionkey")
          .join(customer).where("n_nationkey = s_nationkey")
          .join(orders).where("c_custkey = o_custkey")
          .join(lineitem).where("o_orderkey = l_orderkey")
          .join(supplier).where("l_suppkey = s_suppkey")
          .select("n_name", "l_extendedprice", "l_discount")
      ),

      // Q6 and Q1 is essentially the same

      //    L - O
      //   /     \
      //  S       C
      //  |       |
      // f(N1)  f(N2)
      BenchQuery("q7",
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
          |    n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY'
          |  )
        """.stripMargin,

        nation.as("n1").where("n1.n_name = 'FRANCE'")
          .join(supplier).where("n1.nationkey = s_nationkey")
          .join(lineitem).where("s_suppkey = l_suppkey")
          .join(nation.as("n2").where("n2.n_name = 'GERMANY'")
            .join(customer).where("n2.n_nationkey = c_nationkey")
            .join(orders).where("c_custkey = o_custkey")
          ).where("l_orderkey = o_orderkey")
          .selectExpr("n1.n_name as supp_nation", "n2.n_name as cust_nation", "l_shipdate", "l_extendedprice", "l_discount")
      ),

      //  f(P) -  L - O
      //         /     \
      //        S       C
      //        |       |
      //        N2      N1
      //               /
      //             f(R)
      BenchQuery("q8",
        """
          |select
          |  o_orderdate as o_year, l_extendedprice, l_discount, n2.n_name as nation
          |from
          |  part, supplier, lineitem, orders, customer, nation n1, nation n2, region
          |where
          |  p_partkey = l_partkey
          |  and s_suppkey = l_suppkey
          |  and l_orderkey = o_orderkey
          |  and o_custkey = c_custkey
          |  and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA'
          |  and s_nationkey = n2.n_nationkey
          |  and p_type = 'ECONOMY ANODIZED STEEL'
        """.stripMargin,
        part.where("p_type = 'ECONOMY ANODIZED STEEL'")
          .join(lineitem).where("p_partkey = l_partkey")
          .join(supplier).where("l_suppkey = s_suppkey")
          .join(nation.as("n2")).where("s_nationkey = n2.n_nationkey")
          .join(region.where("r_name = 'AMERICA'")
              .join(nation.as("n1")).where("r_regionkey = n1.n_nationkey")
              .join(customer).where("n1.n_nationkey = c_nationkey")
              .join(orders).where("c_custkey = o_custkey")
          ).where("l_orderkey = o_orderkey")
          .selectExpr("o_orderdate as o_year", "l_extendedprice", "l_discount", "n2.n_name as nation")
      ),

      //    P - L - O
      //       / \
      //      S  PS
      //      |
      //      N
      //
      //        L - O
      //         \
      //      S - PS
      //      |   |
      //      N   P
      BenchQuery("q9a",
        """
          |select
          |  n_name as nation, o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity
          |from
          |  part, supplier, lineitem, partsupp, orders, nation
          |where
          |  s_suppkey = l_suppkey
          |  and ps_suppkey = l_suppkey and ps_partkey = l_partkey
          |  and p_partkey = l_partkey
          |  and o_orderkey = l_orderkey
          |  and s_nationkey = n_nationkey
        """.stripMargin,

        nation
          .join(supplier).where("n_nationkey = s_nationkey")
          .join(lineitem).where("s_suppkey = l_suppkey")
          .join(part).where("l_partkey = p_partkey")
          .join(partsupp).where("l_partkey = ps_partkey and l_suppkey = ps_suppkey")
          .join(orders).where("l_orderkey = o_orderkey")
          .selectExpr("n_name as nation", "o_orderdate", "l_extendedprice", "l_discount", "ps_supplycost", "l_quantity")
      ),

      BenchQuery("q9b",
        """
          |select
          |  n_name as nation, o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity
          |from
          |  part, supplier, lineitem, partsupp, orders, nation
          |where
          |  s_suppkey = l_suppkey
          |  and ps_suppkey = l_suppkey and ps_partkey = l_partkey
          |  and p_partkey = l_partkey
          |  and o_orderkey = l_orderkey
          |  and s_nationkey = n_nationkey
        """.stripMargin,

        nation
          .join(supplier).where("n_nationkey = s_nationkey")
          .join(partsupp).where("s_suppkey = ps_suppkey")
          .join(part).where("p_partkey = ps_partkey")
          .join(lineitem).where("l_partkey = ps_partkey and l_suppkey = ps_suppkey")
          .join(orders).where("l_orderkey = o_orderkey")
          .selectExpr("n_name as nation", "o_orderdate", "l_extendedprice", "l_discount", "ps_supplycost", "l_quantity")
      ),

      // N - C - O - f(L)
      BenchQuery("q10",
        """
          |select
          |  c_custkey, c_name, l_extendedprice, l_discount, c_acctbal, n_name, c_address, c_phone, c_comment
          |from
          |  customer, orders, lineitem, nation
          |where
          |  c_custkey = o_custkey
          |  and l_orderkey = o_orderkey
          |  and l_returnflag = 'R'
          |  and c_nationkey = n_nationkey
        """.stripMargin,

        lineitem.where("l_returnflag = 'R'")
          .join(orders).where("l_orderkey = o_orderkey")
          .join(customer).where("o_custkey = c_custkey")
          .join(nation).where("c_nationkey = n_nationkey")
          .selectExpr("c_custkey", "c_name", "l_extendedprice", "l_discount", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
      ),

      // PS - S - f(N)
      BenchQuery("q11",
        """
          |select
          |  ps_partkey, ps_supplycost, ps_availqty
          |from
          |  partsupp, supplier, nation
          |where
          |  ps_suppkey = s_suppkey
          |  and s_nationkey = n_nationkey
          |  and n_name = 'GERMANY'
        """.stripMargin,
        nation.where("n_name = 'GERMANY'")
          .join(supplier).where("n_nationkey = s_nationkey")
          .join(partsupp).where("s_suppkey = ps_suppkey")
          .select("ps_partkey", "ps_supplycost", "ps_availqty")
      ),

      // O - f(L)
      BenchQuery("q12",
        """
          |select
          |  l_shipmode
          |from
          |  orders,
          |  lineitem
          |where
          |  o_orderkey = l_orderkey
          |  and l_shipmode = 'MAIL'
        """.stripMargin,
        lineitem.where("l_shipmode = 'MAIL'")
          .join(orders).where("l_orderkey = o_orderkey")
          .select("l_shipmode")
      ),

      // q13: do not support outer join
      // C left outer join O
      BenchQuery("q13",
        """
          |select
          |  o_orderkey
          |from
          |  customer left outer join orders on c_custkey = o_custkey
        """.stripMargin,
        orders.join(customer, col("o_custkey") === col("c_custkey"), "right_outer")
          .select("o_orderkey")
      ),

      // L - P
      BenchQuery("q14",
        """
          |select
          |  p_type, l_extendedprice, l_discount
          |from
          |  lineitem, part
          |where
          |  l_partkey = p_partkey
        """.stripMargin,
        part.join(lineitem).where("p_partkey = l_partkey")
          .select("p_type", "l_extendedprice", "l_discount")
      ),

      // L - S
      BenchQuery("q15",
        """
          |select
          |  s_suppkey, s_name, s_address, s_phone
          |from
          |  supplier, lineitem
          |where
          |  s_suppkey = l_suppkey
        """.stripMargin,

        supplier.join(lineitem).where("s_suppkey = l_suppkey")
          .select("s_suppkey", "s_name", "s_address", "s_phone")
      ),

      // PS - f(P)
      BenchQuery("q16",
        """
          |select
          |  p_brand, p_type, p_size, ps_suppkey
          |from
          |  partsupp, part
          |where
          |  p_partkey = ps_partkey
          |  and p_size = 49
        """.stripMargin,
        part.where("p_size = 49")
          .join(partsupp).where("p_partkey = ps_partkey")
          .select("p_brand", "p_type", "p_size", "ps_suppkey")
      ),

      // L - f2(P)
      BenchQuery("q17",
        """
          |select
          |  l_extendedprice
          |from
          |  lineitem, part
          |where
          |  p_partkey = l_partkey
          |  and p_brand = 'Brand#23'
          |  and p_container = 'MED BOX'
        """.stripMargin,

        part.where("p_brand = 'Brand#23' and p_container = 'MED BOX'")
          .join(lineitem).where("p_partkey = l_partkey")
          .select("l_extendedprice")
      ),

      // C - O - L
      BenchQuery("q18",
        """
          |select
          |  c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, l_quantity
          |from
          |  customer, orders, lineitem
          |where
          |  c_custkey = o_custkey and o_orderkey = l_orderkey
        """.stripMargin,

        customer
          .join(orders).where("c_custkey = o_custkey")
          .join(lineitem).where("o_orderkey = l_orderkey")
          .select("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "l_quantity")
      ),

      // f(L) - f(P)
      BenchQuery("q19",
        """
          |select
          | l_extendedprice, l_discount
          |from
          |  lineitem, part
          |where
          |  p_partkey = l_partkey
          |  and p_brand = 'Brand#12'
          |  and p_container = 'SM CASE'
          |  and l_shipmode = 'AIR'
          |  and l_shipinstruct = 'DELIVER IN PERSON'
        """.stripMargin,
        part.where("p_brand = 'Brand#12' and p_container = 'SM CASE'")
          .join(lineitem).where("p_partkey = l_partkey and l_shipmode = 'AIR' and l_shipinstruct = 'DELIVER IN PERSON'")
          .select("l_extendedprice", "l_discount")
      ),

      // f(N) - S left-semi-join PS left-semi-join f(P)
      BenchQuery("q20",
        """
          |select
          |  s_name, s_address
          |from
          |  supplier, nation
          |where s_suppkey in (
          |  select ps_suppkey
          |  from partsupp
          |  where ps_partkey in (
          |    select p_partkey
          |    from part
          |    where p_size = 15
          |  )
          |)
          |  and s_nationkey = n_nationkey
          |  and n_name = 'CANADA'
          |)
        """.stripMargin,
        part.where("p_size = 15")
          .join(partsupp, col("p_partkey") === col("ps_partkey"), "right_semi")
          .join(
            nation.where("n_name = 'CANADA'")
              .join(supplier).where("n_nationkey = s_nationkey"),
            col("ps_suppkey") === col("s_suppkey"),
            "right_semi"
          ).select("s_name", "s_address")
      ),

      //                    L1 left-semi-join L2
      //                    / \
      //            f(N) - S  f(O)
      // self-join
      BenchQuery("q21a",
        """
          |select
          |  s_name
          |from
          |  supplier, lineitem l1, orders, nation
          |where
          |  s_suppkey = l1.l_suppkey
          |  and o_orderkey = l1.l_orderkey
          |  and o_orderstatus = 'F'
          |  and exists (
          |    select *
          |    from
          |      lineitem l2
          |    where
          |      l2.l_orderkey = l1.l_orderkey
          |  )
          |  and s_nationkey = n_nationkey
          |  and n_name = 'SAUDI ARABIA'
        """.stripMargin,
          lineitem.as("l2")
            .join(orders.where("o_orderstatus = 'F'")
              .join(lineitem.as("l1")).where("o_orderkey = l1.l_orderkey")
              .join(supplier).where("l1.l_suppkey = s_suppkey")
              .join(nation).where("s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'"),
              expr("l2.l_orderkey") === col("o_orderkey"), "right_semi")
            .select("s_name")
      ),

      //   L3 join-anti-left L1
      //                    / \
      //            f(N) - S  f(O)
      // self-join
      BenchQuery("q21b",
        """
          |select
          |  s_name
          |from
          |  supplier, lineitem l1, orders, nation
          |where
          |  s_suppkey = l1.l_suppkey
          |  and o_orderkey = l1.l_orderkey
          |  and o_orderstatus = 'F'
          |  and not exists (
          |    select *
          |    from
          |      lineitem l3
          |    where
          |      l3.l_orderkey = l1.l_orderkey
          |  )
          |  and s_nationkey = n_nationkey
          |  and n_name = 'SAUDI ARABIA'
        """.stripMargin,
        lineitem.as("l3")
          .join(orders.where("o_orderstatus = 'F'")
            .join(lineitem.as("l1")).where("o_orderkey = l1.l_orderkey")
            .join(supplier).where("l1.l_suppkey = s_suppkey")
            .join(nation).where("s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'"),
            expr("l3.l_orderkey") === col("o_orderkey"), "right_anti")
          .select("s_name")
      ),

      // C left-anti-join O
      BenchQuery("q22",
        """
          |select
          |  c_phone, c_acctbal
          |from
          |  customer
          |where not exists (
          |  select *
          |  from orders
          |  where o_custkey = c_custkey
          |)
        """.stripMargin,
        orders.join(customer, col("o_custkey") === col("c_custkey"), "right_anti")
          .select("c_phone", "c_acctbal")
      )
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
