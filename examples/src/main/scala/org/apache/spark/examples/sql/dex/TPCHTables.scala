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

import org.apache.spark.sql.SQLContext
// scalastyle:off

class TPCHTables(
                  sqlContext: SQLContext,
                  dbgenDir: String,
                  scaleFactor: String,
                  seed: String,
                  useDoubleForDecimal: Boolean = false,
                  useStringForDate: Boolean = false,
                  generatorParams: Seq[String] = Nil)
  extends Tables(sqlContext, scaleFactor, seed, useDoubleForDecimal, useStringForDate) {
  import sqlContext.implicits._

  val dataGenerator = new DBGEN(dbgenDir, generatorParams)

  val tables = Seq(
    Table("part",
      partitionColumns = "p_brand" :: Nil,
      'p_partkey.long,
      'p_name.string,
      'p_mfgr.string,
      'p_brand.string,
      'p_type.string,
      'p_size.int,
      'p_container.string,
      'p_retailprice.decimal(12, 2),
      'p_comment.string
    ),
    Table("supplier",
      partitionColumns = Nil,
      's_suppkey.long,
      's_name.string,
      's_address.string,
      's_nationkey.long,
      's_phone.string,
      's_acctbal.decimal(12, 2),
      's_comment.string
    ),
    Table("partsupp",
      partitionColumns = Nil,
      'ps_partkey.long,
      'ps_suppkey.long,
      'ps_availqty.int,
      'ps_supplycost.decimal(12, 2),
      'ps_comment.string
    ),
    Table("customer",
      partitionColumns = "c_mktsegment" :: Nil,
      'c_custkey.long,
      'c_name.string,
      'c_address.string,
      'c_nationkey.long,
      'c_phone.string,
      'c_acctbal.decimal(12, 2),
      'c_mktsegment.string,
      'c_comment.string
    ),
    Table("orders",
      partitionColumns = "o_orderdate" :: Nil,
      'o_orderkey.long,
      'o_custkey.long,
      'o_orderstatus.string,
      'o_totalprice.decimal(12, 2),
      'o_orderdate.date,
      'o_orderpriority.string,
      'o_clerk.string,
      'o_shippriority.int,
      'o_comment.string
    ),
    Table("lineitem",
      partitionColumns = "l_shipdate" :: Nil,
      'l_orderkey.long,
      'l_partkey.long,
      'l_suppkey.long,
      'l_linenumber.int,
      'l_quantity.decimal(12, 2),
      'l_extendedprice.decimal(12, 2),
      'l_discount.decimal(12, 2),
      'l_tax.decimal(12, 2),
      'l_returnflag.string,
      'l_linestatus.string,
      'l_shipdate.date,
      'l_commitdate.date,
      'l_receiptdate.date,
      'l_shipinstruct.string,
      'l_shipmode.string,
      'l_comment.string
    ),
    Table("nation",
      partitionColumns = Nil,
      'n_nationkey.long,
      'n_name.string,
      'n_regionkey.long,
      'n_comment.string
    ),
    Table("region",
      partitionColumns = Nil,
      'r_regionkey.long,
      'r_name.string,
      'r_comment.string
    )
  ).map(_.convertTypes())
}