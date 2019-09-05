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

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

trait DexQueryTest extends QueryTest with SharedSQLContext with BeforeAndAfter with PrivateMethodTester {
  val urlEnc = "jdbc:postgresql://localhost:8433/test_edb"
  val url = "jdbc:postgresql://localhost:7433/test_db"
  // val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null
  var connEnc: java.sql.Connection = null
  val properties = new Properties()
  properties.setProperty("Driver", "org.postgresql.Driver")

  lazy val data2 = spark.read.jdbc(url, "testdata2", properties)
  lazy val data3 = spark.read.jdbc(url, "testdata3", properties)
  lazy val data4 = spark.read.jdbc(url, "testdata4", properties)

  protected override def sparkConf = super.sparkConf
    .set(SQLConf.DEX_ENCRYPTED_DATASOURCE_JDBC_URL, urlEnc)
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)

  protected override def afterAll(): Unit = {
    conn.close()
    connEnc.close()
  }

  // Whether to materialize all encrypted test data before the first test is run
  protected def provideEncryptedData: Boolean

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    Utils.classForName("org.postgresql.Driver")

    conn = DriverManager.getConnection(url, properties)
    connEnc = DriverManager.getConnection(urlEnc, properties)
    conn.setAutoCommit(false)
    connEnc.setAutoCommit(false)

    conn.prepareStatement("drop table if exists testdata2").executeUpdate()
    conn.prepareStatement("drop table if exists testdata3").executeUpdate()
    conn.prepareStatement("drop table if exists testdata4").executeUpdate()
    connEnc.prepareStatement("drop table if exists testdata2_prf").executeUpdate()
    connEnc.prepareStatement("drop table if exists testdata3_prf").executeUpdate()
    connEnc.prepareStatement("drop table if exists testdata4_prf").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_filter").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_correlated_join").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_uncorrelated_join").executeUpdate()
    connEnc.prepareStatement("drop table if exists t_domain").executeUpdate()
    conn.commit()
    connEnc.commit()

    conn.prepareStatement("create table testdata2 (a int, b int)")
      .executeUpdate()
    conn.prepareStatement(
      """
        |insert into testdata2 values
        |(1, 1),
        |(1, 2),
        |(2, 1),
        |(2, 2),
        |(3, 1),
        |(3, 2)
      """.stripMargin)
        .executeUpdate()
    conn.commit()

    conn.prepareStatement("create table testdata3 (c int, d int)")
      .executeUpdate()
    conn.prepareStatement(
      """
        |insert into testdata3 values
        |(1, 1),
        |(1, 2),
        |(2, 3)
      """.stripMargin)
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("create table testdata4 (e int, f int)")
      .executeUpdate()
    conn.prepareStatement(
      """
        |insert into testdata4 values
        |(2, 1),
        |(2, 2),
        |(2, 3),
        |(3, 4),
        |(3, 5)
      """.stripMargin)
      .executeUpdate()
    conn.commit()

    if (provideEncryptedData) {
      connEnc.prepareStatement("create table testdata2_prf (rid varchar, a_prf varchar, b_prf varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into testdata2_prf values
          |('1', '1_enc', '1_enc'),
          |('2', '1_enc', '2_enc'),
          |('3', '2_enc', '1_enc'),
          |('4', '2_enc', '2_enc'),
          |('5', '3_enc', '1_enc'),
          |('6', '3_enc', '2_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      connEnc.prepareStatement("create table testdata3_prf (rid varchar, c_prf varchar, d_prf varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into testdata3_prf values
          |('1', '1_enc', '1_enc'),
          |('2', '1_enc', '2_enc'),
          |('3', '2_enc', '3_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      connEnc.prepareStatement("create table testdata4_prf (rid varchar, e_prf varchar, f_prf varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into testdata4_prf values
          |('1', '2_enc', '1_enc'),
          |('2', '2_enc', '2_enc'),
          |('3', '2_enc', '3_enc'),
          |('4', '3_enc', '4_enc'),
          |('5', '3_enc', '5_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      // encrypted multi-map of attr -> domain value
      connEnc.prepareStatement("create table t_domain(label varchar, value varchar)")
          .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into t_domain values
          |('testdata2~a~0', '1_enc'),
          |('testdata2~a~1', '2_enc'),
          |('testdata2~a~2', '3_enc'),
          |
          |('testdata3~c~0', '1_enc'),
          |('testdata3~c~1', '2_enc')
        """.stripMargin)
          .executeUpdate()
      connEnc.commit()

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
      connEnc.prepareStatement("create table t_filter (label varchar, value varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into t_filter values
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

      // encrypted map of (attr1, dom_value1, attr2) -> dom_value2
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
      )

      // encrypted multi-map of (attr, attr, rid) -> rid
      connEnc.prepareStatement("create table t_correlated_join (label varchar, value varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into t_correlated_join values
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
          |('testdata2~b~testdata3~d~6~0', '2_enc')
        """.stripMargin)
        .executeUpdate()
      connEnc.commit()

      // encrypted multi-map of (attr, attr) -> (rid, rid)
      connEnc.prepareStatement("create table t_uncorrelated_join (label varchar, value_left varchar, value_right varchar)")
        .executeUpdate()
      connEnc.prepareStatement(
        """
          |insert into t_uncorrelated_join values
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
