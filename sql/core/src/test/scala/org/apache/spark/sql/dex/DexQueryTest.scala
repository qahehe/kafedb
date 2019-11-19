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

import org.apache.spark.sql.catalyst.dex.DexPrimitives.dexTableNameOf
import org.apache.spark.sql.catalyst.dex.DexConstants.{TableAttributeCompound, tCorrJoinName, tDomainName, tFilterName, tUncorrJoinName}

trait DexQueryTest extends DexTest {
  val data2Name = "testdata2"
  val data3Name = "testdata3"
  val data4Name = "testdata4"
  lazy val data2 = spark.read.jdbc(url, data2Name, properties)
  lazy val data3 = spark.read.jdbc(url, data3Name, properties)
  lazy val data4 = spark.read.jdbc(url, data4Name, properties)

  lazy val compoundKeys = Set(
    TableAttributeCompound(data4Name, Seq("e", "f")), TableAttributeCompound(data2Name, Seq("a", "b"))
  )
  lazy val cks = compoundKeys.map(_.attr)

  /*lazy val fact1 = spark.read.jdbc(url, "test_fact1", properties)
  lazy val dim1 = spark.read.jdbc(url, "test_dim1", properties)
  lazy val dim2 = spark.read.jdbc(url, "test_dim2", properties)
  lazy val dim3 = spark.read.jdbc(url, "test_dim3", properties)

  lazy val fks = Map()*/

  // Whether to materialize all encrypted test data before the first test is run
  protected def provideEncryptedData: Boolean

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    conn.prepareStatement(s"drop table if exists ${data2Name}").executeUpdate()
    conn.prepareStatement(s"drop table if exists ${data3Name}").executeUpdate()
    conn.prepareStatement(s"drop table if exists ${data4Name}").executeUpdate()
    connEnc.prepareStatement(s"drop table if exists ${dexTableNameOf(data2Name)}").executeUpdate()
    connEnc.prepareStatement(s"drop table if exists ${dexTableNameOf(data3Name)}").executeUpdate()
    connEnc.prepareStatement(s"drop table if exists ${dexTableNameOf(data4Name)}").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_filter").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_correlated_join").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_uncorrelated_join").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_domain").executeUpdate()
    conn.commit()
    connEnc.commit()

    conn.prepareStatement(s"create table ${data2Name} (a int, b int)")
      .executeUpdate()
    conn.prepareStatement(
      s"""
        |insert into ${data2Name} values
        |(1, 1),
        |(1, 2),
        |(2, 1),
        |(2, 2),
        |(3, 1),
        |(3, 2)
      """.stripMargin)
        .executeUpdate()
    conn.commit()

    conn.prepareStatement(s"create table ${data3Name} (c int, d int)")
      .executeUpdate()
    conn.prepareStatement(
      s"""
        |insert into ${data3Name} values
        |(1, 1),
        |(1, 2),
        |(2, 3)
      """.stripMargin)
      .executeUpdate()
    conn.commit()

    conn.prepareStatement(s"create table ${data4Name} (e int, f int, g int, h int)")
      .executeUpdate()
    conn.prepareStatement(
      s"""
        |insert into ${data4Name} values
        |(2, 1, 10, 100),
        |(2, 2, 10, 200),
        |(2, 3, 20, 300),
        |(3, 4, 20, 300),
        |(3, 5, 30, 100)
      """.stripMargin)
      .executeUpdate()
    conn.commit()

    /*conn.prepareStatement("create table test_fact1 (f1_pk int, f1_d1_fk int, f1_d2_fk int, f1_a int)")
      .executeUpdate()
    conn.prepareStatement(
      """
        |insert into test_fact1 values
        |(1, 1, 1, 10),
        |(2, 2, 1, 10),
        |(3, 3, 1, 20),
        |(4, 1, 2, 30)
      """.stripMargin)
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("create table test_dim1 (d1_pk int, d1_d3_fk int, d1_a int, d1_b int)")
      .executeUpdate()
    conn.prepareStatement(
      """
        |insert into test_dim1 values
        |(1, 1, 10, 100),
        |(2, 2, 10, 200),
        |(3, 2, 20, 200)
      """.stripMargin)
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("create table test_dim2 (d2_pk int, d2_d3_fk int, d2_a int)")
      .executeUpdate()
    conn.prepareStatement(
      """
        |insert into test_dim2 values
        |(1, 1, 10),
        |(2, 2, 20)
      """.stripMargin)
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("create table test_dim3 (d3_pk int, d3_a int)")
      .executeUpdate()
    conn.prepareStatement(
      """
        |insert into test_dim2 values
        |(1, 100),
        |(2, 200)
      """.stripMargin)
      .executeUpdate()
    conn.commit()*/

    if (provideEncryptedData) {
      // todo: make rid start from 0, to be conssitent with DexBuilder
      connEnc.prepareStatement(s"create table ${dexTableNameOf(data2Name)} (rid varchar, a_prf varchar, b_prf varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        s"""
          |insert into ${dexTableNameOf(data2Name)} values
          |('1', '1_enc', '1_enc'),
          |('2', '1_enc', '2_enc'),
          |('3', '2_enc', '1_enc'),
          |('4', '2_enc', '2_enc'),
          |('5', '3_enc', '1_enc'),
          |('6', '3_enc', '2_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      connEnc.prepareStatement(s"create table ${dexTableNameOf(data3Name)} (rid varchar, c_prf varchar, d_prf varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        s"""
          |insert into ${dexTableNameOf(data3Name)} values
          |('1', '1_enc', '1_enc'),
          |('2', '1_enc', '2_enc'),
          |('3', '2_enc', '3_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      connEnc.prepareStatement(s"create table ${dexTableNameOf(data4Name)} (rid varchar, e_prf varchar, f_prf varchar, g_prf varchar, h_prf varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        s"""
          |insert into ${dexTableNameOf(data4Name)} values
          |('1', '2_enc', '1_enc', '10_enc', '100_enc'),
          |('2', '2_enc', '2_enc', '10_enc', '200_enc'),
          |('3', '2_enc', '3_enc', '20_enc', '300_enc'),
          |('4', '3_enc', '4_enc', '20_enc', '300_enc'),
          |('5', '3_enc', '5_enc', '30_enc', '100_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      // encrypted multi-map of attr -> domain value
      connEnc.prepareStatement(s"create table ${tDomainName}(label varchar, value varchar)")
          .executeUpdate()
      connEnc.prepareStatement(
        s"""
          |insert into ${tDomainName} values
          |('testdata2~a~0', '1_enc'),
          |('testdata2~a~1', '2_enc'),
          |('testdata2~a~2', '3_enc'),
          |
          |('testdata3~c~0', '1_enc'),
          |('testdata3~c~1', '2_enc')
        """.stripMargin)
          .executeUpdate()
      connEnc.commit()

      /*connEnc.prepareStatement("create table t_existence(label varchar)")
          .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into t_existence values
          |('testdata2~1~a~1'), ('testdata2~1~b~1'),
          |('testdata2~2~a~'), ('testdata2~2~b'),
          |('testdata2~3~a~'), ('testdata2~3~b'),
          |('testdata2~4~a~'), ('testdata2~4~b'),
        """.stripMargin)
          .executeUpdate()*/

      /*connEnc.prepareStatement("create table t_fk_pk(label varchar, value varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into t_fkdom values
          |
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      connEnc.prepareStatement("create table t_dom_pk(label varchar, value varchar)")
          .executeUpdate()
      connEnc.prepareStatement(
        """
          |
        """.stripMargin)
          .executeUpdate()
      connEnc.commit()*/

      /*// encrypted set of (rid, domain valuea)
      connEnc.prepareStatement("create table t_correlated_filter(label varchar, value varchar)")
          .executeUpdate()
      // note: just 1_dom_enc would be bad, because two columns can have same domain values
      // adding b~2_dom_enc makes domain values unique to attribute b. Cannot relate to other attribute.
      connEnc.prepareStatement(
        """
          |insert into t_correlated_filter values
          |('testdata2~1~b~1'),
          |('testdata2~2~b~2'),
          |('testdata2~3~b~1'),
          |('testdata2~4~b~2'),
          |('testdata2~5~b~1'),
          |('testdata2~6~b~2')
        """.stripMargin
      )*/
      // t_domain: a -> a-1, a-2, a-3; c -> c-1, c-2.
      // t_filter: a-1 -> [a1, a2], c-1 -> [c1, c2].  So a-1 equi-join c-1 -> [(a1, c1), (a2, c1), (a2, c1), (a2, c2)]
      // selection push down:
      // case 1: filter on joined attr. Filter on t_domain.  a == 1 match a-1? a-2?
      // case 2: filter on unjoined attr.
      //   Choice 1: Filter on t_domain(a) using t_e(b). Gonna do this for post-join filters anyways.
      //   Choice 2: t_filter(b), then what?  t_e(t_filter(b), t_domain(c)) (retain t_domain(c)) join t_filter(retained t_domain(c))
      // advantage: no more quadratic emms
      // multi-way join:
      // case 1: (a join b) join (a join c)
      //   Choice 1: domain_join(a, b) join (domain_join(a, c)
      //   Choice 2: t_e(domain_join(a, b), t_domain(c)) (retain t_domain(c)) join t_filter(retained t_domain(c))

      // encrypted multi-map of (attr, domain value) -> rid
      connEnc.prepareStatement(s"create table ${tFilterName} (label varchar, value varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        s"""
          |insert into ${tFilterName} values
          |('testdata2~a~1~0', '1_enc'),
          |('testdata2~a~1~1', '2_enc'),
          |('testdata2~a~2~0', '3_enc'),
          |('testdata2~a~2~1', '4_enc'),
          |('testdata2~a~3~0', '5_enc'),
          |('testdata2~a~3~1', '6_enc'),
          |
          |('testdata2~b~1~0', '1_enc'),
          |('testdata2~b~1~1', '3_enc'),
          |('testdata2~b~1~2', '5_enc'),
          |('testdata2~b~2~0', '2_enc'),
          |('testdata2~b~2~1', '4_enc'),
          |('testdata2~b~2~2', '6_enc'),
          |
          |('testdata3~c~1~0', '1_enc'),
          |('testdata3~c~1~1', '2_enc'),
          |('testdata3~c~2~0', '3_enc'),
          |
          |('testdata2~a~b~0', '1_enc'),
          |('testdata2~a~b~1', '4_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      /*// encrypted map of (attr1, dom_value1, attr2) -> dom_value2
      // where attr1 and attr2 are from the same table
      connEnc.prepareStatement("create table t_correlated_domain (label varchar, value varchar)")
          .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into t_correlated_domain values
          |('testdata4~e~2~testdata4~b~0', '1_enc'),
          |('testdata4~e~2~testdata4~b~1', '2_enc'),
          |('testdata4~e~2~testdata4~b~2', '3_enc'),
          |('testdata4~e~3~testdata4~b~0', '4_enc'),
          |('testdata4~e~3~testdata4~b~1', '5_enc')
        """.stripMargin
      )*/

      // encrypted multi-map of (attr, attr, rid) -> rid
      connEnc.prepareStatement(s"create table ${tCorrJoinName} (label varchar, value varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        s"""
          |insert into ${tCorrJoinName} values
          |('testdata2~a~testdata4~e~3~0', '1_enc'),
          |('testdata2~a~testdata4~e~3~1', '2_enc'),
          |('testdata2~a~testdata4~e~3~2', '3_enc'),
          |('testdata2~a~testdata4~e~4~0', '1_enc'),
          |('testdata2~a~testdata4~e~4~1', '2_enc'),
          |('testdata2~a~testdata4~e~4~2', '3_enc'),
          |('testdata2~a~testdata4~e~5~0', '4_enc'),
          |('testdata2~a~testdata4~e~5~1', '5_enc'),
          |('testdata2~a~testdata4~e~6~0', '4_enc'),
          |('testdata2~a~testdata4~e~6~1', '5_enc'),
          |
          |('testdata2~a~testdata3~c~1~0', '1_enc'),
          |('testdata2~a~testdata3~c~1~1', '2_enc'),
          |('testdata2~a~testdata3~c~2~0', '1_enc'),
          |('testdata2~a~testdata3~c~2~1', '2_enc'),
          |('testdata2~a~testdata3~c~3~0', '3_enc'),
          |('testdata2~a~testdata3~c~4~0', '3_enc'),
          |
          |('testdata2~b~testdata3~c~1~0', '1_enc'),
          |('testdata2~b~testdata3~c~1~1', '2_enc'),
          |('testdata2~b~testdata3~c~2~0', '3_enc'),
          |('testdata2~b~testdata3~c~3~0', '1_enc'),
          |('testdata2~b~testdata3~c~3~1', '2_enc'),
          |('testdata2~b~testdata3~c~4~0', '3_enc'),
          |('testdata2~b~testdata3~c~5~0', '1_enc'),
          |('testdata2~b~testdata3~c~5~1', '2_enc'),
          |('testdata2~b~testdata3~c~6~0', '3_enc'),
          |
          |('testdata2~b~testdata3~d~1~0', '1_enc'),
          |('testdata2~b~testdata3~d~2~0', '2_enc'),
          |('testdata2~b~testdata3~d~3~0', '1_enc'),
          |('testdata2~b~testdata3~d~4~0', '2_enc'),
          |('testdata2~b~testdata3~d~5~0', '1_enc'),
          |('testdata2~b~testdata3~d~6~0', '2_enc'),
          |
          |('testdata3~c~testdata3~c~1~0', '1_enc'),
          |('testdata3~c~testdata3~c~1~1', '2_enc'),
          |('testdata3~c~testdata3~c~2~0', '1_enc'),
          |('testdata3~c~testdata3~c~2~1', '2_enc'),
          |('testdata3~c~testdata3~c~3~0', '3_enc'),
          |
          |('testdata3~c~testdata4~e~3~0', '1_enc'),
          |('testdata3~c~testdata4~e~3~1', '2_enc'),
          |('testdata3~c~testdata4~e~3~2', '3_enc'),
          |
          |('testdata4~e_and_f~testdata2~a_and_b~1~0','3_enc'),
          |('testdata4~e_and_f~testdata2~a_and_b~2~0','4_enc'),
          |('testdata2~a_and_b~testdata4~e_and_f~4~0','2_enc'),
          |('testdata2~a_and_b~testdata4~e_and_f~3~0','1_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      // encrypted multi-map of (attr, attr) -> (rid, rid)
      connEnc.prepareStatement(s"create table ${tUncorrJoinName} (label varchar, value_left varchar, value_right varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        s"""
          |insert into ${tUncorrJoinName} values
          |('testdata2~a~testdata4~e~0', '3_enc', '1_enc'),
          |('testdata2~a~testdata4~e~1', '3_enc', '2_enc'),
          |('testdata2~a~testdata4~e~2', '3_enc', '3_enc'),
          |('testdata2~a~testdata4~e~3', '4_enc', '1_enc'),
          |('testdata2~a~testdata4~e~4', '4_enc', '2_enc'),
          |('testdata2~a~testdata4~e~5', '4_enc', '3_enc'),
          |('testdata2~a~testdata4~e~6', '5_enc', '4_enc'),
          |('testdata2~a~testdata4~e~7', '5_enc', '5_enc'),
          |('testdata2~a~testdata4~e~8', '6_enc', '4_enc'),
          |('testdata2~a~testdata4~e~9', '6_enc', '5_enc'),
          |
          |('testdata2~a~testdata3~c~0', '1_enc', '1_enc'),
          |('testdata2~a~testdata3~c~1', '1_enc', '2_enc'),
          |('testdata2~a~testdata3~c~2', '2_enc', '1_enc'),
          |('testdata2~a~testdata3~c~3', '2_enc', '2_enc'),
          |('testdata2~a~testdata3~c~4', '3_enc', '3_enc'),
          |('testdata2~a~testdata3~c~5', '4_enc', '3_enc'),
          |
          |('testdata2~b~testdata3~c~0', '1_enc', '1_enc'),
          |('testdata2~b~testdata3~c~1', '1_enc', '2_enc'),
          |('testdata2~b~testdata3~c~2', '2_enc', '3_enc'),
          |('testdata2~b~testdata3~c~3', '3_enc', '1_enc'),
          |('testdata2~b~testdata3~c~4', '3_enc', '2_enc'),
          |('testdata2~b~testdata3~c~5', '4_enc', '3_enc'),
          |('testdata2~b~testdata3~c~6', '5_enc', '1_enc'),
          |('testdata2~b~testdata3~c~7', '5_enc', '2_enc'),
          |('testdata2~b~testdata3~c~8', '6_enc', '3_enc'),
          |
          |('testdata2~b~testdata3~d~0', '1_enc', '1_enc'),
          |('testdata2~b~testdata3~d~1', '2_enc', '2_enc'),
          |('testdata2~b~testdata3~d~2', '3_enc', '1_enc'),
          |('testdata2~b~testdata3~d~3', '4_enc', '2_enc'),
          |('testdata2~b~testdata3~d~4', '5_enc', '1_enc'),
          |('testdata2~b~testdata3~d~5', '6_enc', '2_enc')
        """.stripMargin
      ).executeUpdate()
      connEnc.commit()
    }
  }
}
