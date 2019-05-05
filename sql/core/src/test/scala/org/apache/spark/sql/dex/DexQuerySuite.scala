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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.exceptions.TestFailedException

class DexQuerySuite extends DexQueryTest {

  private lazy val data2 = spark.read.jdbc(url, "testdata2", properties)
  private lazy val data3 = spark.read.jdbc(url, "testdata3", properties)
  private lazy val data4 = spark.read.jdbc(url, "testdata4", properties)

  private def checkDexFor(query: DataFrame): Unit = {
    query.explain(extended = true)
    //val result = query.collect()
    //println("query: " ++ result.mkString)

    val queryDex = query.dex
    queryDex.explain(extended = true)
    //val resultDex = queryDex.collect()
    //println("dex: " ++ resultDex.mkString)

    checkAnswer(queryDex, query)
  }

  test("one filter") {
    val query = data2.select("b").where("a == 2")
    checkDexFor(query)
  }

  test("mix dex and non-dex query") {
    val queryDex = data2.select("b").where("a == 2").dex
    queryDex.explain(extended = true)

    val queryMix = queryDex.selectExpr("b * 2")
    queryMix.explain(extended = true)
    val result = queryMix.collect()
    println("dex: " ++ result.mkString)
    checkAnswer(queryMix, Row(2) :: Row(4):: Nil)
  }

  test("one filter one join") {
    val query = data2.join(data3).where("a == 2 and b == c")
    checkDexFor(query)
  }

  test("one filter one join: transitive attributes, join fully concides with filters") {
    val query = data2.join(data3).where("a == 1 and a == c")
    checkDexFor(query)
  }

  test("conjunctive filters") {
    val query = data2.where("a == 2 and b == 1")
    checkDexFor(query)
  }

  test("disjunctive filters") {
    val query = data2.where("a == 2 or b == 1")
    checkDexFor(query)
  }

  test("IN filter") {
    val query = data2.where("a in (1, 2)")
    checkDexFor(query)
  }

  test("NOT filter") {
    val query = data2.where("a != 2")
    checkDexFor(query)
  }

  test("one join") {
    val query = data2.join(data3).where("b == c")
    checkDexFor(query)
  }

  test("cross join") {
    val query = data2.crossJoin(data3)
    checkDexFor(query)
  }

  test("disjunctive joins: same tables") {
    val query = data2.join(data3).where("a == c or b == d")
    checkDexFor(query)
  }

  test("disjunctive joins: same tables, transitive attrs") {
    val query = data2.join(data3).where("a == c or b == c")
    checkDexFor(query)
  }

  test("two joins: same tables") {
    val query = data2.join(data3).where("a == c and b == d")
    checkDexFor(query)
  }

  test("two joins: same tables, transitive attributes") {
    // inferred a == b same-table filter (not a join!)
    val query = data2.join(data3).where("a == c and b == c")
    checkDexFor(query)
  }

  test("join partially coincides with filters") {
    val query = data2.join(data3).where("a == c and b == d and a = 1")
    checkDexFor(query)
  }

  test("two joins: three tables star schema") {
    val query = data2.join(data4).join(data3).where("a == e and b == c")
    checkDexFor(query)
  }

  test("two joins: three tables star schema transitive attributes") {
    val query = data2.join(data4).join(data3).where("a == e and a == c")
    checkDexFor(query)
  }

  test("jdbc rdd internal rows are unmaterialized cursors") {
    val data3Rdd = spark.sessionState.executePlan(data3.logicalPlan).toRdd
    // wrong
    val mapUnmaterialized = data3Rdd.keyBy(row => row.getInt(0)).values

    // correct: need explicit copy to materialize
    val materialized = data3Rdd.map(row => row.copy())
    val mapMaterialized = materialized.keyBy(row => row.getInt(0)).values

    val expected = Seq((1, 1), (1, 2), (2, 3))

    def check(actual: RDD[InternalRow]): Unit = {
      val actualDf = spark.createDataFrame(actual.collect().map(row => (row.getInt(0), row.getInt(1))))
      val expectedDf = spark.createDataFrame(expected)
      checkAnswer(actualDf, expectedDf)
    }

    check(mapMaterialized)
    assertThrows[TestFailedException](check(mapUnmaterialized))
  }
}
