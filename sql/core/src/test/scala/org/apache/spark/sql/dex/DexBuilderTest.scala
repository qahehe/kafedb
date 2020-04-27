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