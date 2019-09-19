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

import org.apache.spark.sql.dex.DexBuilder.{ForeignKey, PrimaryKey}
import org.apache.spark.sql.dex.DexConstants.{TableAttributeAtom, TableAttributeCompound}
// scalastyle:off

class DexPkFkBuilderTest extends DexTPCHTest {
  override protected def provideEncryptedData: Boolean = true

  test("dex pkfk builder") {
    val dexPkfkBuilder = spark.sessionState.dexBuilder

    val nameToDf = Map(
      "partsupp" -> partsupp,
      "part" -> part,
      "supplier" -> supplier
    )

    val primaryKeys = Set(
      PrimaryKey(TableAttributeCompound("partsupp", Seq("ps_partkey", "ps_suppkey"))),
      PrimaryKey(TableAttributeAtom("part", "p_partkey")),
      PrimaryKey(TableAttributeAtom("supplier", "s_suppkey"))
    )
    val foreignKeys = Set(
      ForeignKey(TableAttributeAtom("partsupp", "ps_partkey"), TableAttributeAtom("part", "p_partkey")),
      ForeignKey(TableAttributeAtom("partsupp", "ps_suppkey"), TableAttributeAtom("supplier", "s_suppkey"))
    )

    dexPkfkBuilder.buildPkFkSchemeFromData(nameToDf, primaryKeys, foreignKeys)

    val partsuppDex = spark.read.jdbc(urlEnc, "partsupp_prf", properties)
    val partDex = spark.read.jdbc(urlEnc, "part_prf", properties)
    val supplierDex = spark.read.jdbc(urlEnc, "supplier_prf", properties)

    println("partsuppDex: \n" + partsuppDex.columns.mkString(",") + "\n" + partsuppDex.collect().mkString("\n"))
    println("partDex: \n" + partDex.columns.mkString(",") + "\n" + partDex.collect().mkString("\n"))
    println("supplierDex: \n" + supplierDex.columns.mkString(",") + "\n" + supplierDex.collect().mkString("\n"))
  }
}
