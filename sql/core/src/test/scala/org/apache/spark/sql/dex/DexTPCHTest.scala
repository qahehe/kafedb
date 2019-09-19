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
package org.apache.spark.sql.dex
// scalastyle:off

import java.sql.Connection

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

trait DexTPCHTest extends QueryTest with DexTest {

  lazy val partsupp = spark.read.jdbc(url, "partsupp", properties)
  lazy val part     = spark.read.jdbc(url, "part", properties)
  lazy val supplier = spark.read.jdbc(url, "supplier", properties)
  //lazy val nation   = spark.read.jdbc(url, "nation", properties)
  //lazy val region   = spark.read.jdbc(url, "region", properties)

  lazy val primaryKeys = Set(
    "p_partkey",
    "s_suppkey"
  )
  lazy val foreignKeys = Set(
    "ps_partkey",
    "ps_suppkey"
  )

  protected def provideEncryptedData: Boolean

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    def dropIfExists(c: Connection, tableName: String) = {
      c.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
    def execute(c: Connection, query: String) = {
      c.prepareStatement(query).executeUpdate()
    }


    dropIfExists(conn, "partsupp")
    dropIfExists(conn, "part")
    dropIfExists(conn, "supplier")
    dropIfExists(connEnc, "partsupp_prf")
    dropIfExists(connEnc, "part_prf")
    dropIfExists(connEnc, "supplier_prf")

    execute(conn, "create table partsupp (ps_partkey int, ps_suppkey int, ps_comment varchar)")
    execute(conn,
      """
        |insert into partsupp values
        |(1, 1, 'psa'),
        |(2, 2, 'psa'),
        |(1, 2, 'psb'),
        |(3, 3, 'psb'),
        |(4, 3, 'psb'),
        |(3, 3, 'psc')
      """.stripMargin)

    execute(conn, "create table part (p_partkey int, p_name varchar)")
    execute(conn,
      """
        |insert into part values
        |(1, 'pa'),
        |(2, 'pa'),
        |(3, 'pb'),
        |(4, 'pb')
      """.stripMargin)

    execute(conn, "create table supplier (s_suppkey int, s_name varchar, s_address varchar)")
    execute(conn,
      """
        |insert into supplier values
        |(1, 'sa', 'sa1'),
        |(2, 'sb', 'sa1'),
        |(3, 'sb', 'sa2')
      """.stripMargin)

    // todo: in case primary key is meaningful and needs to conceal, one can easily create pseudo primary key
    // and then map the pseudo to real primary key via 'enc(key=f(table,pseudo_pk), real_pk)'.
    // For now, assume all primary keys are already pseudo.
    // Note: fk-pk join (e.g. fpk_partsupp_part_prf) is encrytped using specific search tokens.  The actual fk attribute
    // (if required to be queryable) is encrypted like any data cell.
    execute(connEnc,
      """
        |create table partsupp_prf (
        |  rid                       varchar,
        |
        |  pfk_part_partsupp_prf     varchar,
        |  fpk_partsupp_part_prf     varchar,
        |  ps_partkey_prf            varchar,
        |
        |  pfk_supplier_partsupp_prf varchar,
        |  fpk_partsupp_supplier_prf varchar,
        |  ps_suppkey_prf            varchar,
        |
        |  val_partsupp_ps_comment_prf varchar,
        |  ps_comment_prf              varchar
        |)
      """.stripMargin)
    execute(connEnc,
      """
        |insert into partsupp_prf values
        |('11', 'part~partsupp~1~0', '1_enc_partsupp~part~11', '1_enc', 'supplier~partsupp~1~0', '1_enc_partsupp~supplier~11', '1_enc', 'partsupp~ps_comment~psa~0', 'psa_enc'),
        |('22', 'part~partsupp~2~0', '2_enc_partsupp~part~22', '2_enc', 'supplier~partsupp~2~0', '2_enc_partsupp~supplier~22', '2_enc', 'partsupp~ps_comment~psa~1', 'psa_enc'),
        |('31', 'part~partsupp~1~1', '1_enc_partsupp~part~31', '1_enc', 'supplier~partsupp~2~1', '2_enc_partsupp~supplier~31', '2_enc', 'partsupp~ps_comment~psb~0', 'psb_enc'),
        |('43', 'part~partsupp~3~0', '3_enc_partsupp~part~43', '3_enc', 'supplier~partsupp~3~0', '3_enc_partsupp~supplier~43', '3_enc', 'partsupp~ps_comment~psb~1', 'psb_enc'),
        |('54', 'part~partsupp~4~0', '4_enc_partsupp~part~54', '4_enc', 'supplier~partsupp~3~1', '3_enc_partsupp~supplier~54', '3_enc', 'partsupp~ps_comment~psb~2', 'psb_enc'),
        |('63', 'part~partsupp~3~1', '3_enc_partsupp~part~63', '3_enc', 'supplier~partsupp~3~2', '3_enc_partsupp~supplier~63', '3_enc', 'partsupp~ps_comment~psc~0', 'psc_enc')
      """.stripMargin)

    execute(connEnc,
      """
        |create table part_prf (
        |  rid bigint,
        |  p_partkey_prf varchar,
        |
        |  val_part_p_name_prf varchar,
        |  p_name_prf varchar
        |)
      """.stripMargin)
    execute(connEnc,
      """
        |insert into part_prf values
        |(1, '1_enc', 'part~p_name~pa~0', 'pa_enc'),
        |(2, '2_enc', 'part~p_name~pa~1', 'pa_enc'),
        |(3, '3_enc', 'part~p_name~pb~0', 'pb_enc'),
        |(4, '4_enc', 'part~p_name~pb~1', 'pb_enc')
      """.stripMargin)

    execute(connEnc,
      """
        |create table supplier_prf (
        |  rid bigint,
        |  s_suppkey_prf varchar,
        |
        |  val_supplier_s_name_prf varchar,
        |  s_name_prf varchar,
        |
        |  val_supplier_s_address_prf varchar,
        |  s_address_prf varchar
        |)
      """.stripMargin)
    execute(connEnc,
      """
        |insert into supplier_prf values
        |(1, '1_enc', 'supplier~s_name~sa~0', 'sa_enc', 'supplier~s_address~sa1~0', 'sa1_enc'),
        |(2, '2_enc', 'supplier~s_name~sb~0', 'sb_enc', 'supplier~s_address~sa1~1', 'sa1_enc'),
        |(3, '3_enc', 'supplier~s_name~sb~1', 'sb_enc', 'supplier~s_address~sa2~0', 'sa2_enc')
      """.stripMargin)

    conn.commit()
    connEnc.commit()

  }
}
