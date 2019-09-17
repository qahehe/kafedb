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

import org.apache.spark.sql.dex.DexConstants.TableAttribute
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.BeforeAndAfter


class DexBuilderTest extends DexQueryTest with SharedSQLContext with BeforeAndAfter {
  override protected def provideEncryptedData: Boolean = false

  test("dex builder") {
    val dexBuilder = spark.sessionState.dexBuilder

    val nameToDf = Map(
      "testdata2" -> data2,
      "testdata3" -> data3,
      "testdata4" -> data4
    )

    val joins = Seq(
      (TableAttribute("testdata2", "a"), TableAttribute("testdata3", "c"))
    )

    dexBuilder.buildFromData(nameToDf, joins)

    val testData2Enc = spark.read.jdbc(urlEnc, "testdata2_prf", properties)
    val tFilter = spark.read.jdbc(urlEnc, "t_filter", properties)
    val tDomain = spark.read.jdbc(urlEnc, "t_domain", properties)
    val tUncorrJoin = spark.read.jdbc(urlEnc, "t_uncorrelated_join", properties)
    val tCorrJoin = spark.read.jdbc(urlEnc, "t_correlated_join", properties)

    println("testData2Enc: \n" + testData2Enc.collect().mkString("\n"))
    println("t_filter: \n" + tFilter.collect().mkString("\n"))
    println("t_domain: \n" + tDomain.collect().mkString("\n"))
    println("t_uncorrelated_join: \n" + tUncorrJoin.collect().mkString("\n"))
    println("t_correlated_join: \n" + tCorrJoin.collect().mkString("\n"))
  }
}
