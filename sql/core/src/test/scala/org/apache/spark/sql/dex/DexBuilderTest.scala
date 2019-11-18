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
import org.apache.spark.sql.dex.DexPrimitives.dexTableNameOf


class DexBuilderTest extends DexQueryTest {
  override protected def provideEncryptedData: Boolean = false

  lazy val dexBuilder = spark.sessionState.dexBuilder

  lazy val nameToDf = Map(
    "testdata2" -> data2,
    "testdata3" -> data3,
    "testdata4" -> data4
  )

  lazy val primaryKeys = Set(
    PrimaryKey(TableAttributeCompound("testdata2", Seq("a", "b"))),
    PrimaryKey(TableAttributeAtom("testdata3", "d")),
    PrimaryKey(TableAttributeAtom("testdata4", "g"))
  )

  lazy val foreignKeys = Set(
    ForeignKey(TableAttributeAtom("testdata4", "e"), TableAttributeAtom("testdata3", "d")),
    ForeignKey(TableAttributeCompound("testdata4", Seq("e", "f")), TableAttributeCompound("testdata2", Seq("a", "b")))
  )

  test("dex builder: spx") {
    dexBuilder.buildFromData(DexVariant.from("DexSpx").asInstanceOf[DexStandalone], nameToDf, primaryKeys, foreignKeys)
    Seq("testdata2_prf", "testdata3_prf", "testdata4_prf", DexConstants.tFilterName, DexConstants.tUncorrJoinName).foreach { t =>
      val df = spark.read.jdbc(urlEnc, t, properties)
      println(t + ": \n"
        + df.columns.mkString(",") + "\n"
        + df.collect().mkString("\n"))
    }
  }

  test("dex builder: corr") {
    dexBuilder.buildFromData(DexVariant.from("dexcorr").asInstanceOf[DexStandalone], nameToDf, primaryKeys, foreignKeys)
/*    Seq(dexTableNameOf("testdata2"),
      dexTableNameOf("testdata3"),
      dexTableNameOf("testdata4"),
      DexConstants.tFilterName,
      DexConstants.tCorrJoinName).foreach { t =>
      val df = spark.read.jdbc(urlEnc, t, properties)
      println(t + ": \n"
        + df.columns.mkString(",") + "\n"
        + df.collect().mkString("\n"))
    }*/

    Seq(dexTableNameOf("testdata2"),
      dexTableNameOf("testdata3"),
      dexTableNameOf("testdata4")).foreach { t =>
      val df = spark.read.jdbc(urlEnc, t, properties)
      println(t + ":")
      println(df.columns.mkString(","))
      println(
        df.collect().map { x =>
        Range(0, x.length).collect {
          case i if df.columns(i) == "rid" =>
            x.getLong(i)
          case i =>
            assert(df.dtypes(i)._2 == "BinaryType")
            DataCodec.decode[String](Crypto.symDec(DexPrimitives.masterSecret.aesKey, x.get(i).asInstanceOf[Array[Byte]]))
        }.mkString(",")
      }.mkString("\n"))
    }
  }
}
