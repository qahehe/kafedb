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

package org.apache.spark.sql.catalyst.plans.logical
// scalastyle:off

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}

case class DexRidFilter(predicate: String, emm: LogicalPlan) extends UnaryNode {

  // todo: for t_m of joinning a new table, need to add new rid to output
  override def output: Seq[Attribute] = emm.output.collect {
    case x: Attribute if x.name == "label" => x.withName("value_dec_key")
    case x => x
  }

  override def child: LogicalPlan = emm

  // for input attributes that this plan depends on
  // by default, all the Expressions present in the CashTSelect(...), see super.references
  // want to avoid declare emm the attributes to the ChasTSelect(...)
  // but if we don't add them here, they can be optimized away by optimizer
  // e.g. tselect.rid can be projected away immediately because they think
  // we're not using it anywhere in the query plan, though we do in doExecute()
  // syntatically we need to let the optimizer know
  override def references: AttributeSet = super.references ++ AttributeSet(output)
}

case class DexRidCorrelatedJoin(predicate: String, childView: LogicalPlan, emm: LogicalPlan, childViewRid: Attribute) extends BinaryNode {
  override def left: LogicalPlan = childView

  override def right: LogicalPlan = emm

  override def output: Seq[Attribute] = left.output ++ (right.output.collect {
    case x: Attribute if x.name == "label" => x.withName("value_dec_key")
    case x => x
  })

  override def references: AttributeSet = super.references ++ AttributeSet(output)
}

