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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

trait DexTest extends QueryTest with SharedSQLContext with BeforeAndAfter with PrivateMethodTester {
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

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    Utils.classForName("org.postgresql.Driver")

    conn = DriverManager.getConnection(url, properties)
    connEnc = DriverManager.getConnection(urlEnc, properties)
    conn.setAutoCommit(false)
    connEnc.setAutoCommit(false)
  }


  protected def checkDexFor(query: DataFrame, queryDex: DataFrame): Unit = {
    //query.explain(extended = true)
    //val result = query.collect()
    //println("query: " ++ result.mkString)

    queryDex.explain(extended = true)
    //val resultDex = queryDex.collect()
    //println("dex: " ++ resultDex.mkString)

    checkAnswer(queryDex, query)
  }
}
