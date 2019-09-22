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

class DexTPCHSuite extends DexTPCHTest {
  override protected def provideEncryptedData: Boolean = true

  test("one filter") {
    val query = part.where("p_name == 'pb'")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two conjunctive filters") {
    val query = supplier.where("s_name == 'sb' and s_address == 'sa1'")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one join: foreign key to primary key") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one join: primary key to foreign key") {
    val query = supplier.join(partsupp).where("s_suppkey == ps_suppkey")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one pk-fk join one filter on pk table") {
    val query = supplier.join(partsupp).where("s_suppkey == ps_suppkey and s_name == 'sa'")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one pk-fk join one filter on fk table") {
    val query = supplier.join(partsupp).where("s_suppkey == ps_suppkey and ps_comment == 'psb'")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one fk-pk join one filter on pk table") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey and s_name == 'sa'")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one fk-pk join one filter on fk table") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey and ps_comment == 'psb'")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: fk-pk and fk-pk") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey").join(part).where("ps_partkey == p_partkey")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: pk-fk and pk-fk") {
    val query = supplier.join(part.join(partsupp).where("p_partkey == ps_partkey")).where("s_suppkey == ps_suppkey")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: fk-pk and pk-fk") {
    val query = part.join(partsupp.join(supplier).where("ps_suppkey == s_suppkey")).where("p_partkey == ps_partkey")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: pk-fk and fk-pk") {
    val query = part.join(partsupp).where("p_partkey == ps_partkey").join(supplier).where("ps_suppkey == s_suppkey")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  ignore("compound key join") {
    val query = partsupp.join(lineitem).where("ps_partkey = l_partkey and ps_suppkey = l_suppkey")
    val queryDex = partsupp.join(lineitem).where("ps_partkey_and_ps_suppkey = l_partkey_and_l_suppkey").select("ps_comment", "l_comment").dexPkFk(pks, fks)
    checkDexFor(query, queryDex)
  }
}
