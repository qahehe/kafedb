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

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.optimizer.PushDownPredicate
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class EncryptPlanSuite extends EncryptPlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Understanding batch", Once,
        PushDownPredicate
        ) :: Nil
  }

  object Encrypt extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Encrypt Constant Filter", Once, EncryptConstantFilter) ::
        Batch("Encrypt Projection", Once, EncryptProjection) :: Nil
  }


  test("encrypt constant filter") {
    val originalQuery = table("t1").select('a).where('a === 1)
    val analyzed = analyzer.execute(originalQuery)
    val optimized = Optimize.execute(analyzed)
    val encrypted = Encrypt.execute(optimized)
    val encryptedAnalyzed = analyzer.execute(encrypted)
    // scalastyle:off
    println(originalQuery)
    println(analyzed)
    println(optimized)
    println(encrypted)
    println(encryptedAnalyzed)
    // scalastyle:on
  }
}
