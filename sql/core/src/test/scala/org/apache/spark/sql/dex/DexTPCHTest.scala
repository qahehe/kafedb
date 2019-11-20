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
import org.apache.spark.sql.dex.DexBuilder.{ForeignKey, PrimaryKey}
import org.apache.spark.sql.catalyst.dex.DexConstants.{TableAttributeAtom, TableAttributeCompound}

trait DexTPCHTest extends QueryTest with DexTest {

  lazy val partsupp = spark.read.jdbc(url, "partsupp", properties)
  lazy val part     = spark.read.jdbc(url, "part", properties)
  lazy val supplier = spark.read.jdbc(url, "supplier", properties)
  lazy val lineitem = spark.read.jdbc(url, "lineitem", properties)
  //lazy val nation   = spark.read.jdbc(url, "nation", properties)
  //lazy val region   = spark.read.jdbc(url, "region", properties)

  lazy val primaryKeys = Set(
    PrimaryKey(TableAttributeAtom("part", "p_partkey")),
    PrimaryKey(TableAttributeAtom("supplier", "s_suppkey")),
    PrimaryKey(TableAttributeCompound("partsupp", Seq("ps_partkey", "ps_suppkey"))),
    PrimaryKey(TableAttributeCompound("lineitem", Seq("l_orderkey", "l_linenumber")))
  )
  lazy val foreignKeys = Set(
    ForeignKey(TableAttributeAtom("partsupp", "ps_partkey"), TableAttributeAtom("part", "p_partkey")),
    ForeignKey(TableAttributeAtom("partsupp", "ps_suppkey"), TableAttributeAtom("supplier", "s_suppkey")),
    ForeignKey(
      TableAttributeCompound("lineitem", Seq("l_partkey", "l_suppkey")),
      TableAttributeCompound("partsupp", Seq("ps_partkey", "ps_suppkey"))),
    ForeignKey(TableAttributeAtom("lineitem", "l_partkey"), TableAttributeAtom("part", "p_partkey")),
    ForeignKey(TableAttributeAtom("lineitem", "l_suppkey"), TableAttributeAtom("supplier", "s_suppkey"))
  )
  lazy val compoundKeys = Set(
    TableAttributeCompound("partsupp", Seq("ps_partkey", "ps_suppkey")),
    TableAttributeCompound("lineitem", Seq("l_orderkey", "l_linenumber"))
  )
  lazy val pks = primaryKeys.map(_.attr.attr)
  lazy val fks = foreignKeys.map(_.attr.attr)
  lazy val cks = compoundKeys.map(_.attr)

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
    dropIfExists(conn, "lineitem")
    dropIfExists(connEnc, "partsupp_prf")
    dropIfExists(connEnc, "part_prf")
    dropIfExists(connEnc, "supplier_prf")
    dropIfExists(connEnc, "lineitem_prf")

    execute(conn, "create table partsupp (ps_partkey int, ps_suppkey int, ps_comment varchar)")
    execute(conn,
      """
        |insert into partsupp values
        |(1, 1, 'psa'),
        |(2, 2, 'psa'),
        |(1, 2, 'psb'),
        |(3, 3, 'psb'),
        |(4, 3, 'psb'),
        |(2, 3, 'psc')
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

    execute(conn, "create table lineitem (l_orderkey int, l_linenumber int, l_partkey int, l_suppkey int, l_comment varchar)")
    execute(conn,
      """
        |insert into lineitem values
        |(1, 1, 1, 1, 'la1'),
        |(1, 2, 1, 1, 'la2'),
        |(2, 1, 2, 2, 'la3'),
        |(2, 2, 2, 2, 'la3')
      """.stripMargin)

    conn.commit()

    if (provideEncryptedData){
    // todo: in case primary key is meaningful and needs to conceal, one can easily create pseudo primary key
    // and then map the pseudo to real primary key via 'enc(key=f(table,pseudo_pk), real_pk)'.
    // For now, assume all primary keys are already pseudo.
    // Note: fk-pk join (e.g. fpk_partsupp_part_prf) is encrytped using specific search tokens.  The actual fk attribute
    // (if required to be queryable) is encrypted like any data cell.
    //
    // Include foreign key and atom primary key as encrypted attributes for debugging only.
    execute(connEnc,
      """
        |create table partsupp_prf (
        |  rid                       bigint,
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
        |(1 * 2 + 1 * 3, 'part~partsupp~1~0', '1_enc_partsupp~part~5',  '1_enc', 'supplier~partsupp~1~0', '1_enc_partsupp~supplier~5',  '1_enc', 'partsupp~ps_comment~psa~0', 'psa_enc'),
        |(2 * 2 + 2 * 3, 'part~partsupp~2~0', '2_enc_partsupp~part~10', '2_enc', 'supplier~partsupp~2~0', '2_enc_partsupp~supplier~10', '2_enc', 'partsupp~ps_comment~psa~1', 'psa_enc'),
        |(1 * 2 + 2 * 3, 'part~partsupp~1~1', '1_enc_partsupp~part~8',  '1_enc', 'supplier~partsupp~2~1', '2_enc_partsupp~supplier~8',  '2_enc', 'partsupp~ps_comment~psb~0', 'psb_enc'),
        |(3 * 2 + 3 * 3, 'part~partsupp~3~0', '3_enc_partsupp~part~15', '3_enc', 'supplier~partsupp~3~0', '3_enc_partsupp~supplier~15', '3_enc', 'partsupp~ps_comment~psb~1', 'psb_enc'),
        |(4 * 2 + 3 * 3, 'part~partsupp~4~0', '4_enc_partsupp~part~17', '4_enc', 'supplier~partsupp~3~1', '3_enc_partsupp~supplier~17', '3_enc', 'partsupp~ps_comment~psb~2', 'psb_enc'),
        |(2 * 2 + 3 * 3, 'part~partsupp~2~1', '2_enc_partsupp~part~13', '2_enc', 'supplier~partsupp~3~2', '3_enc_partsupp~supplier~13', '3_enc', 'partsupp~ps_comment~psc~0', 'psc_enc')
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

    execute(connEnc,
      """
        |create table lineitem_prf (
        |  rid bigint,
        |
        |  pfk_partsupp_lineitem_prf varchar,
        |  fpk_lineitem_partsupp_prf varchar,
        |
        |  pfk_part_lineitem_prf varchar,
        |  fpk_lineitem_part_prf varchar,
        |
        |  val_lineitem_l_comment_prf varchar,
        |  l_comment_prf varchar
        |)
      """.stripMargin)
    execute(connEnc,
      """
        |insert into lineitem_prf values
        |(1 * 2 + 1 * 3, 'partsupp~lineitem~5~0',  '5_enc_lineitem~partsupp~5',   'part~lineitem~1~0', '1_enc_lineitem~part~5',  'lineitem~l_comment~la1~0', 'la1_enc'),
        |(1 * 2 + 2 * 3, 'partsupp~lineitem~5~1',  '5_enc_lineitem~partsupp~8',   'part~lineitem~1~1', '1_enc_lineitem~part~8',  'lineitem~l_comment~la2~0', 'la2_enc'),
        |(2 * 2 + 1 * 3, 'partsupp~lineitem~10~0', '10_enc_lineitem~partsupp~7',  'part~lineitem~2~0', '2_enc_lineitem~part~7',  'lineitem~l_comment~la3~0', 'la3_enc'),
        |(2 * 2 + 2 * 3, 'partsupp~lineitem~10~1', '10_enc_lineitem~partsupp~10', 'part~lineitem~2~1', '2_enc_lineitem~part~10', 'lineitem~l_comment~la3~1', 'la3_enc')
      """.stripMargin)

      connEnc.commit()
    }
  }
}
