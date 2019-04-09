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

import org.apache.spark.sql.dex.DexTestData.{TestData2Enc, TestTSelect}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

trait DexQueryTest extends QueryTest with SharedSQLContext with BeforeAndAfter with PrivateMethodTester {
  import testImplicits._

  val urlEnc = "jdbc:postgresql://localhost:8433/test_edb"
  val url = "jdbc:postgresql://localhost:7433/test_db"
  // val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null
  var connEnc: java.sql.Connection = null
  val properties = new Properties()
  properties.setProperty("Driver", "org.postgresql.Driver")

  protected override def sparkConf = super.sparkConf
    .set(SQLConf.DEX_ENCRYPTED_DATASOURCE_JDBC_URL, urlEnc)

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
    connEnc.prepareStatement("drop table if exists testdata2_prf").executeUpdate()
    connEnc.prepareStatement("drop table if exists tselect").executeUpdate()
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

    connEnc.prepareStatement("create table tselect (rid varchar, value varchar)")
      .executeUpdate()
    connEnc.prepareStatement(
      """
        |insert into tselect values
        |('testdata2~a~1~0', 'r1_enc'),
        |('testdata2~a~1~1', 'r2_enc'),
        |('testdata2~a~2~0', 'r3_enc'),
        |('testdata2~a~2~1', 'r4_enc'),
        |('testdata2~a~3~0', 'r5_enc'),
        |('testdata2~a~3~1', 'r6_enc')
      """.stripMargin)
      .executeUpdate()
    connEnc.commit()

    /*if (provideEncryptedData) {
      testData2Enc
      testTSelect
    }*/
  }

  /*protected lazy val testData2Enc: DataFrame = {
    val df = spark.sparkContext.parallelize(
      TestData2Enc("r1", "1_enc", "1_enc") ::
        TestData2Enc("r2", "1_enc", "2_enc") ::
        TestData2Enc("r3", "2_enc", "1_enc") ::
        TestData2Enc("r4", "2_enc", "2_enc") ::
        TestData2Enc("r5", "3_enc", "1_enc") ::
        TestData2Enc("r6", "3_enc", "2_enc") :: Nil, 2).toDF()
    df.createOrReplaceTempView("testData2_enc")
    df
  }

  protected lazy val testTSelect: DataFrame = {
    val df = spark.sparkContext.parallelize(
      TestTSelect("testData2~a~1~counter", "r1_enc") ::
        TestTSelect("testData2~a~1~counter", "r2_enc") ::
        TestTSelect("testData2~a~2~counter", "r3_enc") ::
        TestTSelect("testData2~a~2~counter", "r4_enc") ::
        TestTSelect("testData2~a~3~counter", "r5_enc") ::
        TestTSelect("testData2~a~3~counter", "r6_enc") :: Nil, 2).toDF()
    df.createOrReplaceTempView("t_select")
    df
  }

  def setupEncryptedData(): Unit = {
    provideEncryptedData = true
  }*/
}

object DexTestData {
  case class TestData2Enc(rid: String, a_prf: String, b_prf: String)
  case class TestTSelect(rid: String, value: String)
}
