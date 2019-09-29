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
import org.apache.spark.sql.functions

class DexQuerySuite extends DexQueryTest {

  override protected def provideEncryptedData: Boolean = true

  test("one filter") {
    val query = data2.select("b").where("a == 2")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("mix dex and non-dex query") {
    val query =  data2.select("b").where("a == 2")
    val queryMix = query.dexCorr(cks).agg(functions.min("b"))
    checkAnswer(query.agg(functions.min("b")), queryMix)
  }

  test("one filter one join") {
    val query = data2.join(data3).where("a == 2 and b == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("one filter one join: transitive attributes, join fully concides with filters") {
    val query = data2.join(data3).where("a == 1 and a == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("conjunctive filters") {
    val query = data2.where("a == 2 and b == 1")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("disjunctive filters") {
    val query = data2.where("a == 2 or b == 1")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("IN filter") {
    val query = data2.where("a in (1, 2)")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("NOT filter") {
    val query = data2.where("a != 2")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("one join") {
    val query = data2.join(data3).where("b == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("one join one filter, nonoverlap") {
    val query = data2.join(data3).where("b == d and a == 1")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("one join one filter, nonoverlap, post-join filter") {
    val query = data2.join(data3).where("b == d and c == 1")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("one join one filter, nonoverlap, reverse join pair order") {
    val query = data2.join(data3).where("d == b and a == 1")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("table alias") {
    val query = data2.join(data3.as("d3")).where("b == d3.c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("self join") {
    val query = data3.as("d3a").join(data3.as("d3b")).where("d3a.c == d3b.c").select("d3a.c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("cross join") {
    val query = data2.crossJoin(data3)
    checkDexFor(query, query.dexCorr(cks))
  }

  test("disjunctive joins: same tables") {
    val query = data2.join(data3).where("a == c or b == d")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("disjunctive joins: same tables, transitive attrs") {
    val query = data2.join(data3).where("a == c or b == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("two joins: same tables") {
    val query = data2.join(data3).where("a == c and b == d")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("two joins: same tables, transitive attributes") {
    // inferred a == b same-table filter (not a join!)
    val query = data2.join(data3).where("a == c and b == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("join partially coincides with filters") {
    val query = data2.join(data3).where("a == c and b == d and a = 1")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("two joins: three tables star schema") {
    val query = data2.join(data4).join(data3).where("a == e and b == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("two joins: three tables star schema transitive attributes") {
    val query = data2.join(data4).join(data3).where("a == e and a == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("two joins: nested order") {
    val query = data2.join(
      data3.join(data4).where("c == e")
    ).where("a == c")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("two joins: nested order, not joinning left most table") {
    val query = data2.join(
      data3.join(data4).where("c == e")
    ).where("a == e")
    checkDexFor(query, query.dexCorr(cks))
  }

  test("spx") {
    val query1 = data2.join(data4).where("a == e")
    checkDexFor(query1, query1.dexSpx)

    val query2 = data2.join(data3).where("a == 2 and b == c")
    checkDexFor(query2, query2.dexSpx)


    val query3 = data2.join(data3).where("a == c and b == d")
    checkDexFor(query3, query3.dexSpx)


    val query4 = data2.join(data3).where("a == c and b == c")
    checkDexFor(query4, query4.dexSpx)


    val query5 = data2.join(data3).where("a == c and b == d and a = 1")
    checkDexFor(query5, query5.dexSpx)


    val query6 = data2.join(data4).join(data3).where("a == e and b == c")
    checkDexFor(query6, query6.dexSpx)


    val query7 = data2.join(data4).join(data3).where("a == e and a == c")
    checkDexFor(query7, query7.dexCorr(cks))
  }

  test("dex domain") {
    val query1 = data2.join(data3).where("a == c and b == 2")
    checkDexFor(query1, query1.dexDom)

    val query2 = data2.where("b == 2").join(data3).where("a == c")
    checkDexFor(query2, query2.dexDom)
  }

  test("dex compound key join") {
    val query1 = data2.join(data4).where("a == e and b == f")
    checkDexFor(query1, query1.dexCorr(cks))
  }

  test("jdbc rdd internal rows are unmaterialized cursors") {
    val expected = Seq((1, 1), (1, 2), (2, 3))
    val unsafeRowHeaderRepr = 0

    val data3Rdd = spark.sessionState.executePlan(data3.logicalPlan).toRdd
    //println(data3Rdd.collect().mkString)
    //println(data3Rdd.toDebugString)

    // why is it wrong? Conjecture: rdd.collect() returns array of UnsafeRows, which are references to the same
    // cursor. The cursor iterates and updates all the referred UnsafeRows eventually to the last row.
    val keyByThenValues = data3Rdd.map(row => (row.getInt(0), row)).map(_._2).collect()
    //assert(expectedWithInternalRowHeader !== keyByThenValues)
    val expectedWrong = expected.map(_ => InternalRow(unsafeRowHeaderRepr, expected.last._1, expected.last._2)).toArray
    assert(expectedWrong.mkString === keyByThenValues.mkString)

    // Getters copies out the row values under the cursor, so they no longer get updated by the cursor
    val keyByThenGet = data3Rdd.map(row => (row.getInt(0), row)).map(row => (row._2.getInt(0), row._2.getInt(1))).collect()
    assert(expected.mkString === keyByThenGet.mkString)

    // Getters can happen down the one-one (aka "narrow") dependency
    val keyByThenValuesThenGet = data3Rdd.map(row => (row.getInt(0), row)).map(_._2).map(row => (row.getInt(0), row.getInt(1))).collect()
    assert(expected.mkString === keyByThenValuesThenGet.mkString)

    // Wide dependency: (wrong) just shuffling JDBC cursors
    val shuffleJdbcRow = data3Rdd.map(row => (row.getInt(0), row)).groupByKey().map(_._2).collect()
    println(shuffleJdbcRow.mkString)

    // Wide dependency: (wrong) not only shuffling, but also multiplying (copying) JDBC cursors
    val multiplyJdbcRow: RDD[(Int, (Int, InternalRow))] = sparkContext.parallelize(expected).join(data3Rdd.map(row => (row.getInt(0), row)))
    println(multiplyJdbcRow.collect().mkString)

    // Wide dependency: (wrong) copy after shuffling
    val shuffleJdbcRowThenCopy = data3Rdd.map(row => (row.getInt(0), row)).groupByKey().map(rows => rows.copy()).collect()
    println(shuffleJdbcRowThenCopy.mkString)

    // Wide dependency: copy BEFORE shuffling
    val shuffleCopiedJdbcRow = data3Rdd.map(row => (row.getInt(0), row.copy())).groupByKey().map(_._2).collect()
    println(shuffleCopiedJdbcRow.mkString)

    val multiplyCopiedJdbcRow: RDD[(Int, (Int, InternalRow))] = sparkContext.parallelize(expected).join(data3Rdd.map(row => (row.getInt(0), row.copy)))
    println(multiplyCopiedJdbcRow.collect().mkString)

  }
}
