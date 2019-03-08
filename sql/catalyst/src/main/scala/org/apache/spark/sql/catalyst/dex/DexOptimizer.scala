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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer

// An optimizer that is used before encryption
// It needs to preserve subquery aliases (table names) to as to help encryption
class DexOptimizer(catalog: SessionCatalog) extends Optimizer(catalog) {

  val dexExcludedRules: Seq[String] = Seq(EliminateSubqueryAliases.ruleName)

  override def excludedRulesConf: Seq[String] = super.excludedRulesConf ++ dexExcludedRules

  override def nonExcludableRules: Seq[String] =
    super.nonExcludableRules.filterNot(dexExcludedRules.contains(_))
}
