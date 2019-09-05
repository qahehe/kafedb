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
import org.apache.spark.sql.catalyst.dsl.expressions._

abstract class DexUnaryOperator(predicate: String, emm: LogicalPlan) extends UnaryNode {
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

case class DexRidFilter(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(predicate, emm)

case class SpxRidUncorrelatedJoin(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(predicate, emm)

case class DexDomainValues(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(predicate, emm)

case class DexCorrelatedDomainsValues(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(predicate, emm)

case class DexDomainFilter(predicate: String, predicateAttr: Attribute, childView: LogicalPlan) extends UnaryNode {
  override def child: LogicalPlan = childView

  override def output: Seq[Attribute] = ???
}

abstract class DexBinaryOperator(childView: LogicalPlan, emm: LogicalPlan) extends BinaryNode {
  override def left: LogicalPlan = childView

  override def right: LogicalPlan = emm

  override def output: Seq[Attribute] = left.output ++ right.output.collect {
    case x: Attribute if x.name == "label" => x.withName("value_dec_key")
    case x => x
  }

  override def references: AttributeSet = super.references ++ AttributeSet(output)
}

case class DexRidCorrelatedJoin(predicate: String, childView: LogicalPlan, emm: LogicalPlan, childViewRid: Attribute) extends DexBinaryOperator(childView, emm)

case class DexDomainRids(predicatePrefix: String, domainValues: LogicalPlan, emm: LogicalPlan, domainValueAttr: Attribute) extends DexBinaryOperator(domainValues, emm)

case class DexDomainJoin(leftPredicate: String, rightPredicate: String, intersectedDomainValues: LogicalPlan, emmLeft: LogicalPlan, emmRight: LogicalPlan) extends TertiaryNode {
  override def left: LogicalPlan = emmLeft

  override def middle: LogicalPlan = intersectedDomainValues

  override def right: LogicalPlan = emmRight

  override def output: Seq[Attribute] = middle.output ++ left.output.collect {
    case x: Attribute if x.name == "label" => x.withName("value_dec_key_left")
    case x: Attribute if x.name == "value" => x.withName("value_left")
  } ++ right.output.collect {
    case x: Attribute if x.name == "label" => x.withName("value_dec_key_right")
    case x: Attribute if x.name == "value" => x.withName("value_right")
  }
}