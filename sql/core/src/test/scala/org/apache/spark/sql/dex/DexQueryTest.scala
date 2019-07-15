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

  protected override def sparkConf = super.sparkConf
    .set(SQLConf.DEX_ENCRYPTED_DATASOURCE_JDBC_URL, urlEnc)
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)

  protected override def afterAll(): Unit = {
    conn.close()
    connEnc.close()
  }

  // Whether to materialize all encrypted test data before the first test is run
  private var provideEncryptedData = false

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
    connEnc.prepareStatement("drop table if exists tselect").executeUpdate()
    connEnc.prepareStatement("drop table if exists tm").executeUpdate()
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

    connEnc.prepareStatement("create table testdata2_prf (rid varchar, a_prf varchar, b_prf varchar)")
        .executeUpdate()
    connEnc.prepareStatement(
      """
        |insert into testdata2_prf values
        |('r1', '1_enc', '1_enc'),
        |('r2', '1_enc', '2_enc'),
        |('r3', '2_enc', '1_enc'),
        |('r4', '2_enc', '2_enc'),
        |('r5', '3_enc', '1_enc'),
        |('r6', '3_enc', '2_enc')
      """.stripMargin)
      .executeUpdate()
    connEnc.commit()

    connEnc.prepareStatement("create table testdata3_prf (rid varchar, c_prf varchar, d_prf varchar)")
      .executeUpdate()
    connEnc.prepareStatement(
      """
        |insert into testdata3_prf values
        |('r1', '1_enc', '1_enc'),
        |('r2', '1_enc', '2_enc'),
        |('r3', '2_enc', '3_enc')
      """.stripMargin)
      .executeUpdate()
    connEnc.commit()

    connEnc.prepareStatement("create table testdata4_prf (rid varchar, e_prf varchar, f_prf varchar)")
      .executeUpdate()
    connEnc.prepareStatement(
      """
        |insert into testdata4_prf values
        |('r1', '2_enc', '1_enc'),
        |('r2', '2_enc', '2_enc'),
        |('r3', '2_enc', '3_enc'),
        |('r4', '3_enc', '4_enc'),
        |('r5', '3_enc', '5_enc')
      """.stripMargin)
      .executeUpdate()
    connEnc.commit()

    connEnc.prepareStatement("create table tselect (label varchar, value varchar)")
      .executeUpdate()
    connEnc.prepareStatement(
      """
        |insert into tselect values
        |('testdata2~a~1~0', 'r1_enc'),
        |('testdata2~a~1~1', 'r2_enc'),
        |('testdata2~a~2~0', 'r3_enc'),
        |('testdata2~a~2~1', 'r4_enc'),
        |('testdata2~a~3~0', 'r5_enc'),
        |('testdata2~a~3~1', 'r6_enc'),
        |
        |('testdata2~b~1~0', 'r1_enc'),
        |('testdata2~b~1~1', 'r3_enc'),
        |('testdata2~b~1~2', 'r5_enc'),
        |('testdata2~b~2~0', 'r2_enc'),
        |('testdata2~b~2~1', 'r4_enc'),
        |('testdata2~b~2~2', 'r6_enc'),
        |
        |('testdata3~c~1~0', 'r1_enc'),
        |('testdata3~c~1~1', 'r2_enc'),
        |('testdata3~c~2~0', 'r3_enc'),
        |
        |('testdata2~a~b~0', 'r1_enc'),
        |('testdata2~a~b~1', 'r4_enc')
      """.stripMargin)
      .executeUpdate()
    connEnc.commit()

    connEnc.prepareStatement("create table tm (label varchar, value varchar)")
      .executeUpdate()
    connEnc.prepareStatement(
      """
        |insert into tm values
        |('testdata2~a~testdata4~e~r3~0', 'r1_enc'),
        |('testdata2~a~testdata4~e~r3~1', 'r2_enc'),
        |('testdata2~a~testdata4~e~r3~2', 'r3_enc'),
        |('testdata2~a~testdata4~e~r4~0', 'r1_enc'),
        |('testdata2~a~testdata4~e~r4~1', 'r2_enc'),
        |('testdata2~a~testdata4~e~r4~2', 'r3_enc'),
        |('testdata2~a~testdata4~e~r5~0', 'r4_enc'),
        |('testdata2~a~testdata4~e~r5~1', 'r5_enc'),
        |('testdata2~a~testdata4~e~r6~0', 'r4_enc'),
        |('testdata2~a~testdata4~e~r6~1', 'r5_enc'),
        |
        |('testdata2~a~testdata3~c~r1~0', 'r1_enc'),
        |('testdata2~a~testdata3~c~r1~1', 'r2_enc'),
        |('testdata2~a~testdata3~c~r2~0', 'r1_enc'),
        |('testdata2~a~testdata3~c~r2~1', 'r2_enc'),
        |('testdata2~a~testdata3~c~r3~0', 'r3_enc'),
        |('testdata2~a~testdata3~c~r4~0', 'r3_enc'),
        |
        |('testdata2~b~testdata3~c~r1~0', 'r1_enc'),
        |('testdata2~b~testdata3~c~r1~1', 'r2_enc'),
        |('testdata2~b~testdata3~c~r2~0', 'r3_enc'),
        |('testdata2~b~testdata3~c~r3~0', 'r1_enc'),
        |('testdata2~b~testdata3~c~r3~1', 'r2_enc'),
        |('testdata2~b~testdata3~c~r4~0', 'r3_enc'),
        |('testdata2~b~testdata3~c~r5~0', 'r1_enc'),
        |('testdata2~b~testdata3~c~r5~1', 'r2_enc'),
        |('testdata2~b~testdata3~c~r6~0', 'r3_enc'),
        |
        |('testdata2~b~testdata3~d~r1~0', 'r1_enc'),
        |('testdata2~b~testdata3~d~r2~0', 'r2_enc'),
        |('testdata2~b~testdata3~d~r3~0', 'r1_enc'),
        |('testdata2~b~testdata3~d~r4~0', 'r2_enc'),
        |('testdata2~b~testdata3~d~r5~0', 'r1_enc'),
        |('testdata2~b~testdata3~d~r6~0', 'r2_enc')
      """.stripMargin)
      .executeUpdate()
    connEnc.commit()
  }
}
