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

import org.apache.spark.sql.dex.DexBuilder.{ForeignKey, PrimaryKey}
import org.apache.spark.sql.dex.DexConstants.{TableAttributeAtom, TableAttributeCompound}


class DexBuilderTest extends DexQueryTest {
  override protected def provideEncryptedData: Boolean = false

  test("dex builder") {
    val dexBuilder = spark.sessionState.dexBuilder

    val nameToDf = Map(
      "testdata2" -> data2,
      "testdata3" -> data3,
      "testdata4" -> data4
    )

    val primaryKeys = Set(
      PrimaryKey(TableAttributeCompound("testdata2", Seq("a", "b"))),
      PrimaryKey(TableAttributeAtom("testdata3", "d")),
      PrimaryKey(TableAttributeAtom("testdata4", "f"))
    )

    val foreignKeys = Set(
      ForeignKey(TableAttributeAtom("testdata2", "a"), TableAttributeAtom("testdata3", "c")),
      ForeignKey(TableAttributeCompound("testdata4", Seq("e", "f")), TableAttributeCompound("testdata2", Seq("a", "b")))
    )

    dexBuilder.buildFromData(nameToDf, primaryKeys, foreignKeys)

    Seq("testdata2_prf", "testdata3_prf", "testdata4_prf", "t_filter", "t_correlated_join").foreach { t =>
      val df = spark.read.jdbc(urlEnc, t, properties)
      println(t + ": \n"
        + df.columns.mkString(",") + "\n"
        + df.collect().mkString("\n"))
    }
  }
}
