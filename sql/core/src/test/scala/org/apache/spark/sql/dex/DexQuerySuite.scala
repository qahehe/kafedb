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

import org.apache.spark.sql.Row
// scalastyle:off

class DexQuerySuite extends DexQueryTest {

  test("one filter") {
    val query = spark.read.jdbc(url, "testdata2", properties).select("b").where("a == 2")
    val queryDex = spark.read.jdbc(url, "testdata2", properties).select("b").where("a == 2").dex
    queryDex.explain(extended = true)
    val result = queryDex.collect()
    println(result.mkString)
    checkAnswer(queryDex, query)
  }

  test("mix dex and non-dex query") {
    val queryDex = spark.read.jdbc(url, "testdata2", properties).select("b").where("a == 2").dex
    val queryMix = queryDex.selectExpr("b * 2")
    checkAnswer(queryMix, Row(2) :: Row(4):: Nil)
  }

  test("one filter one join") {
    val testData2 = spark.read.jdbc(url, "testdata2", properties)
    val testData3 = spark.read.jdbc(url, "testdata3", properties)
    val query = testData2.join(testData3).where("a == 2 and b == c")
    query.explain(extended = true)
    val result = query.collect()
    println("query: " ++ result.mkString)

    val queryDex = query.dex
    queryDex.explain(extended = true)
    val resultDex = queryDex.collect()
    println("dex: " ++ resultDex.mkString)

    checkAnswer(queryDex, query)
  }
}
