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

case class BenchQuery(name: String, query: String, queryDf: DataFrame)
case class BenchQueryResult(name: String, resultCount: Long, duration: Long)

object TPCHPredicates extends DexTPCHBenchCommon {

  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    val variant = BenchVariant.from(args(0))

    val queries = Seq(
      BenchQuery("f1", "select * from region where r_name = 'EUROPE'",
        region.where("r_name = 'EUROPE'")),

      BenchQuery("f2", "select * from part where p_size = 15",
        part.where("p_size = 15")),

      BenchQuery("f3", "select * from part where p_type = 'ECONOMY ANODIZED STEEL'",
        part.where("p_type = 'ECONOMY ANODIZED STEEL'")),

      BenchQuery("f4", "select * from nation where n_name = 'FRANCE'",
        nation.where("n_name = 'FRANCE'")),

      BenchQuery("f5", "select * from lineitem where l_returnflag = 'R'",
        lineitem.where("l_returnflag = 'R'")),

      BenchQuery("f6", "select * from lineitem where l_shipmode = 'MAIL'",
        lineitem.where("l_shipmode = 'MAIL'")),

      BenchQuery("j1", "select * from part, partsupp where p_partkey = ps_partkey",
        part.join(partsupp).where("p_partkey = ps_partkey")),

      BenchQuery("j2", "select * from supplier, partsupp where s_suppkey = ps_suppkey",
        supplier.join(partsupp).where("s_suppkey = ps_suppkey")),

      BenchQuery("j3", "select * from region, nation where r_regionkey = n_regionkey",
        region.join(nation).where("r_regionkey = n_regionkey")),

      BenchQuery("j4", "select * from nation, customer where n_nationkey = c_nationkey",
        nation.join(customer).where("n_nationkey = c_nationkey")),

      BenchQuery("j5", "select * from nation, supplier where n_nationkey = s_nationkey",
        nation.join(supplier).where("n_nationkey = s_nationkey")),

      BenchQuery("j6", "select * from partsupp, lineitem where ps_partkey = l_partkey and ps_suppkey = l_suppkey",
        partsupp.join(lineitem).where("ps_partkey = l_partkey and ps_suppkey = l_suppkey")),

      BenchQuery("j7", "select * from customer, orders where c_custkey = o_custkey",
        customer.join(orders).where("c_custkey = o_custkey")),

      BenchQuery("j8", "select * from orders, lineitem where o_orderkey = l_orderkey",
        orders.join(lineitem).where("o_orderkey = l_orderkey")),

      BenchQuery("j9", "select * from part, lineitem where p_partkey = l_partkey",
        part.join(lineitem).where("p_partkey = l_partkey")),

      BenchQuery("j10", "select * from supplier, lineitem where s_suppkey = l_suppkey",
        supplier.join(lineitem).where("s_suppkey = l_suppkey")),

      // f(P) - F,  P=small, F=Big
      BenchQuery("b1a", "select * from part, partsupp where p_size = 15 and p_partkey = ps_partkey",
        part.join(partsupp).where("p_size = 15 and p_partkey = ps_partkey")),

      // F - f(P)
      BenchQuery("b1b", "select * from partsupp, part where ps_partkey = p_partkey and p_size = 15",
        partsupp.join(part).where("ps_partkey = p_partkey and p_size = 15")),

      // P - f(F)
      BenchQuery("b2a", "select * from orders, lineitem where o_orderkey = l_orderkey and l_returnflag = 'R'",
        orders.join(lineitem).where("o_orderkey = l_orderkey and l_returnflag = 'R'")),

      // f(F) - P
      BenchQuery("b2b", "select * from lineitem, orders where l_returnflag = 'R' and l_orderkey = o_orderkey",
        lineitem.join(orders).where("l_returnflag = 'R' and l_orderkey = o_orderkey")),

      // F - P, large P
      BenchQuery("j8b", "select * from lineitem, orders where l_orderkey = o_orderkey",
        lineitem.join(orders).where("l_orderkey = o_orderkey")),

      // F - P, medium P
      BenchQuery("j9b", "select * from lineitem, part where l_partkey = p_partkey",
        lineitem.join(part).where("l_partkey = p_partkey")),

      // almost a triangular query: R(A, B), S(B, C), T(A, C) = R join(B) S and S join(C) T and R join (A) T
      // more like: R(A), S(A, B), T(A, C)
      // F - F => F - P - F, small P
      //       F
      //     /
      //   P
      //     \
      //       F
      BenchQuery("t1a", "select * from supplier, nation, customer where s_nationkey = n_nationkey and n_nationkey = c_nationkey",
        supplier
          .join(nation).where("s_nationkey = n_nationkey")
          .join(customer).where("n_nationkey = c_nationkey")),

      // F - F => P - F - F, small P
      BenchQuery("t1b", "select * from nation, supplier, customer where n_nationkey = s_nationkey and n_nationkey = c_nationkey",
        nation
          .join(supplier).where("n_nationkey = s_nationkey")
          .join(customer).where("n_nationkey = c_nationkey")),

      // F - F => F - (F - P), small P
      BenchQuery("t1c", "select * from supplier, nation, customer where s_nationkey = n_nationkey and c_nationkey = n_nationkey",
        customer
          .join(supplier
            .join(nation).where("s_nationkey = n_nationkey")
        ).where("c_nationkey = n_nationkey")),

      // almost a triangular query. More like R(A, B), S(B, C), T(A, D*).
      //   P
      //  /
      // CP
      //  \
      //   P
      // CP - P - P
      BenchQuery("t2a", "select * from partsupp, part, supplier where ps_partkey = p_partkey and ps_suppkey = s_suppkey",
        partsupp
          .join(part).where("ps_partkey = p_partkey")
          .join(supplier).where("ps_suppkey = s_suppkey")),

      // P - CP - P
      BenchQuery("t2b", "select * from part, partsupp, supplier where p_partkey = ps_partkey and ps_suppkey = s_suppkey",
        part
          .join(partsupp).where("p_partkey = ps_partkey")
          .join(supplier).where("ps_suppkey = s_suppkey")),

      // P - (P - CP)
      BenchQuery("t2c", "select * from part, supplier, partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey",
        supplier
          .join(part
            .join(partsupp).where("p_partkey = ps_partkey")
          ).where("s_suppkey = ps_suppkey")),

      // P - (CP - P)
      BenchQuery("t2d", "select * from part, supplier, partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey",
        supplier
          .join(partsupp
            .join(part).where("ps_partkey = p_partkey")
          ).where("s_suppkey = ps_suppkey")),

      // almost a triangular query. More like R(A, B), S(B, C), T(A, D*).
      //   P
      //  /
      // F
      //  \
      //   P
      // F - P - P
      BenchQuery("t3a", "select * from lineitem, part, supplier where l_partkey = p_partkey and l_suppkey = s_suppkey",
        lineitem
          .join(part).where("l_partkey = p_partkey")
          .join(supplier).where("l_suppkey = s_suppkey")),

      // P - F - P
      BenchQuery("t3b", "select * from part, lineitem, supplier where p_partkey = l_partkey and l_suppkey = s_suppkey",
        part
          .join(lineitem).where("p_partkey = l_partkey")
          .join(supplier).where("l_suppkey = s_suppkey")),

      // P - (P - F)
      BenchQuery("t3c", "select * from part, supplier, lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey",
        supplier
          .join(part
            .join(lineitem).where("p_partkey = l_partkey")
          ).where("s_suppkey = l_suppkey")),

      // P - (F - P)
      BenchQuery("t3d", "select * from part, supplier, lineitem where p_partkey = l_partkey and s_suppkey = l_suppkey",
        supplier
          .join(lineitem
            .join(part).where("l_partkey = p_partkey")
          ).where("s_suppkey = l_suppkey")),

      // like a linearly transitive query
      // F - P_F - P
      BenchQuery("r1a", "select * from lineitem, orders, customer where l_orderkey = o_orderkey and o_custkey = c_custkey",
        lineitem
          .join(orders).where("l_orderkey = o_orderkey")
          .join(customer).where("o_custkey = c_custkey")),

      // F - (P_F - P), intermediate data
      BenchQuery("r1b", "select * from lineitem, orders, customer where o_custkey = c_custkey and l_orderkey = o_orderkey",
        lineitem.join(orders
          .join(customer).where("o_custkey = c_custkey")
        ).where("l_orderkey = o_orderkey")),

      // F - (P - P_F), intermediate data
      BenchQuery("r1c", "select * from lineitem, orders, customer where c_custkey = o_custkey and l_orderkey = o_orderkey",
        lineitem.join(customer
          .join(orders).where("c_custkey = o_custkey")
        ).where("l_orderkey = o_orderkey")),

      // P - F_P - F
      BenchQuery("r1d", "select * from customer, orders, lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey",
        customer
          .join(orders).where("c_custkey = o_custkey")
          .join(lineitem).where("o_orderkey = l_orderkey")),

      // P - (F_P - F), intermediate data
      BenchQuery("r1e", "select * from customer, orders, lineitem where o_orderkey = l_orderkey and c_custkey = o_custkey",
        customer
          .join(orders
            .join(lineitem).where("o_orderkey = l_orderkey")
          ).where("c_custkey = o_custkey")),

      // P - (F - P_F), intermediate data
      BenchQuery("r1f", "select * from customer, lineitem, orders where l_orderkey = o_orderkey and c_custkey = o_custkey",
        customer
          .join(lineitem
            .join(orders).where("l_orderkey = o_orderkey")
          ).where("c_custkey = o_custkey"))
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
