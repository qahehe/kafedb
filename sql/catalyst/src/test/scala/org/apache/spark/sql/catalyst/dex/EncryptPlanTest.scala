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
package org.apache.spark.sql.catalyst.dex

import java.net.URI

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf

trait EncryptPlanTest extends PlanTest {

  protected val analyzer = makeAnalyzer(caseSensitive = true)

  private def makeAnalyzer(caseSensitive: Boolean): Analyzer = {
    val conf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> caseSensitive)
    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, conf)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    catalog.createTempView("t1", DexTestRelations.t1, overrideIfExists = true)
    catalog.createTempView("t1_enc", DexTestRelations.t1Enc, overrideIfExists = true)
    catalog.createTempView("tselect_t1_a", DexTestRelations.tselect_t1_a, overrideIfExists = true)
    new Analyzer(catalog, conf) {
    }
  }

}
