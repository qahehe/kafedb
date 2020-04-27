/*
Copyright 2020, Brown University, Providence, RI.

                        All Rights Reserved

Permission to use, copy, modify, and distribute this software and
its documentation for any purpose other than its incorporation into a
commercial product or service is hereby granted without fee, provided
that the above copyright notice appear in all copies and that both
that copyright notice and this permission notice appear in supporting
documentation, and that the name of Brown University not be used in
advertising or publicity pertaining to distribution of the software
without specific, written prior permission.

BROWN UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR ANY
PARTICULAR PURPOSE.  IN NO EVENT SHALL BROWN UNIVERSITY BE LIABLE FOR
ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package org.apache.spark.sql.dex
// scalastyle:off

import java.sql.Connection

import org.apache.spark.sql.{DataFrame, Dataset, QueryTest}
import org.apache.spark.sql.dex.DexBuilder.{ForeignKey, PrimaryKey}
import org.apache.spark.sql.catalyst.dex.DexConstants.{TableAttributeAtom, TableAttributeCompound}
import org.apache.spark.sql.catalyst.dex.DexPrimitives._

trait DexTPCHTest extends QueryTest with DexTest {

  lazy val partsupp = spark.read.jdbc(url, "partsupp", properties)
  lazy val part     = spark.read.jdbc(url, "part", properties)
  lazy val supplier = spark.read.jdbc(url, "supplier", properties)
  lazy val lineitem = spark.read.jdbc(url, "lineitem", properties)
  //lazy val nation   = spark.read.jdbc(url, "nation", properties)
  //lazy val region   = spark.read.jdbc(url, "region", properties)

  def tableNames: Seq[String] = Seq("partsupp", "part", "supplier", "lineitem")

  // Don't call this function before creating tables
  def nameToDf: Map[String, DataFrame] = Map(
    "partsupp" -> partsupp,
    "part" -> part,
    "supplier" -> supplier,
    "lineitem" -> lineitem
  )

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
    TableAttributeCompound("lineitem", Seq("l_orderkey", "l_linenumber")),
    TableAttributeCompound("lineitem", Seq("l_partkey", "l_suppkey"))
  )
  lazy val pks = primaryKeys.map(_.attr.attr)
  lazy val fks = foreignKeys.map(_.attr.attr)
  lazy val cks = DexBuilder.compoundKeysFrom(primaryKeys, foreignKeys).map(_.attr)

  protected def encryptData(): Unit

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    def dropIfExists(c: Connection, tableName: String) = {
      c.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
    def execute(c: Connection, query: String) = {
      c.prepareStatement(query).executeUpdate()
    }

    tableNames.foreach { t =>
      dropIfExists(conn, t)
      dropIfExists(connEnc, dexTableNameOf(t))
      conn.commit()
      connEnc.commit()
    }

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

    encryptData()
    connEnc.commit()
  }
}
