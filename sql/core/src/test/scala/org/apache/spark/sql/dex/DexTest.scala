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

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
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

  val dialect = JdbcDialects.get(JDBCOptions.JDBC_URL)

  protected override def sparkConf = super.sparkConf
    .set(SQLConf.DEX_ENCRYPTED_DATASOURCE_JDBC_URL, urlEnc)
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)

  protected override def afterAll(): Unit = {
    conn.commit()
    conn.close()
    connEnc.commit()
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
    query.explain(extended = true)
    //val result = query.collect()
    //println("query: " ++ result.mkString)

    queryDex.explain(extended = true)
    //val resultDex = queryDex.collect()
    //println("dex: " ++ resultDex.mkString)

    checkAnswer(queryDex, query)
  }
}
