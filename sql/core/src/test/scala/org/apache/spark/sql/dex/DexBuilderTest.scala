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

import org.apache.spark.sql.catalyst.dex.{Crypto, DataCodec, DexConstants, DexPrimitives}
import org.apache.spark.sql.dex.DexBuilder.{ForeignKey, PrimaryKey}
import org.apache.spark.sql.catalyst.dex.DexConstants.{TableAttributeAtom, TableAttributeCompound}
import org.apache.spark.sql.catalyst.dex.DexPrimitives.dexTableNameOf


class DexSpxBuilderTest extends DexTPCHTest {
  override protected def encryptData(): Unit = {
    lazy val dexBuilder = spark.sessionState.dexBuilder
    dexBuilder.buildFromData(DexVariant.from("DexSpx").asInstanceOf[DexStandalone], nameToDf, primaryKeys, foreignKeys)
  }

  test("dex builder: spx") {

    Seq("testdata2_prf", "testdata3_prf", "testdata4_prf", DexConstants.tFilterName, DexConstants.tUncorrJoinName).foreach { t =>
      val df = spark.read.jdbc(urlEnc, t, properties)
      println(t + ": \n"
        + df.columns.mkString(",") + "\n"
        + df.collect().mkString("\n"))
    }
  }
}

class DexCorrBuilderTest extends DexTPCHTest {
  override protected def encryptData(): Unit = {
    lazy val dexBuilder = spark.sessionState.dexBuilder
    dexBuilder.buildFromData(DexVariant.from("dexcorr").asInstanceOf[DexStandalone], nameToDf, primaryKeys, foreignKeys)
  }

  test("dex builder: corr") {
    Seq(dexTableNameOf("partsupp"),
      dexTableNameOf("part"),
      dexTableNameOf("supplier"),
      DexConstants.tFilterName,
      DexConstants.tCorrJoinName).foreach { t =>
      val df = spark.read.jdbc(urlEnc, t, properties)
      println(t + ": \n"
        + df.columns.mkString(",") + "\n"
        + df.collect().mkString("\n"))
    }

    Seq(dexTableNameOf("testdata2"),
      dexTableNameOf("testdata3"),
      dexTableNameOf("testdata4")).foreach { t =>
      val df = spark.read.jdbc(urlEnc, t, properties)
      println(t + ":")
      //println(df.columns.mkString(","))
      println(df.dtypes.map(x => s"${x._1}:${x._2}").mkString(","))
      println(
        df.collect().map { x =>
          Range(0, x.length).collect {
            case i if df.columns(i) == "rid" =>
              DataCodec.decode[Long](x.getAs[Array[Byte]](i))
            case i =>
              assert(df.dtypes(i)._2 == "BinaryType")
              DataCodec.decode[Int](Crypto.symDec(DexPrimitives.masterSecret.aesKey, x.get(i).asInstanceOf[Array[Byte]]))
          }.mkString(",")
        }.mkString("\n"))
    }
  }

  test("dex cor end to end") {
    val q1 = nameToDf("part").select("p_name")
    checkDexFor(q1, q1.dexCorr(cks))

    val q2 = nameToDf("part").where("p_name = 'pa'").select("p_name")
    checkDexFor(q2, q2.dexCorr(cks))

    val q3 = nameToDf("part").join(nameToDf("partsupp")).where("p_partkey = ps_partkey")
    checkDexFor(q3, q3.dexCorr(cks))

    val q4 = nameToDf("part").join(nameToDf("partsupp")).where("p_name = 'pa' and p_partkey = ps_partkey")
    checkDexFor(q4, q4.dexCorr(cks))

    val q5 = nameToDf("part")
      .join(nameToDf("partsupp")
        .join(nameToDf("supplier"))
        .where("ps_suppkey = s_suppkey")
      ).where("p_partkey = ps_partkey")
    checkDexFor(q5, q5.dexCorr(cks))

    val q6 = nameToDf("part").where("p_name = 'pa'")
      .join(nameToDf("partsupp")
        .join(nameToDf("supplier"))
        .where("ps_suppkey = s_suppkey and s_name = 'sb'")
      ).where("p_partkey = ps_partkey")
    checkDexFor(q6, q6.dexCorr(cks))
  }
}