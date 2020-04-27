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
package org.apache.spark.sql.catalyst.plans.logical
// scalastyle:off

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, DialectSQLTranslatable, Literal}
import org.apache.spark.sql.catalyst.dsl.expressions._

abstract class DexUnaryOperator(emm: LogicalPlan) extends UnaryNode {
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

case class DexRidFilter(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(emm)

case class SpxRidUncorrelatedJoin(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(emm)

case class DexDomainValues(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(emm)

case class DexCorrelatedDomainsValues(predicate: String, emm: LogicalPlan) extends DexUnaryOperator(emm)

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

case class DexRidCorrelatedJoin(predicatePrefix: String, childView: LogicalPlan, emm: LogicalPlan, childViewRid: Attribute) extends DexBinaryOperator(childView, emm)

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

case class DexPseudoPrimaryKeyFilter(predicate: String, labelColumn: String, labelColumnOrder: Attribute, filterTableName: String, filterTable: LogicalPlan) extends UnaryNode {
  override def references: AttributeSet = super.references ++ AttributeSet(output)

  override def output: Seq[Attribute] = filterTable.output

  override def child: LogicalPlan = filterTable // use for analysis resolution of label column only
}

case class DexPseudoPrimaryKeyDependentFilter(predicate: String, labelColumn: String, labelColumnOrder: Attribute, filterTableName: String, filterTable: LogicalPlan, filterTableRid: Attribute) extends UnaryNode {
  override def references: AttributeSet = super.references ++ AttributeSet(output)

  override def output: Seq[Attribute] = Seq(filterTableRid)

  override def child: LogicalPlan = filterTable // use for analysis resolution of label column only
}

sealed abstract class MaterializationStrategy(val sql: String)
case object Materialized extends MaterializationStrategy("MATERIALIZED")
case object NotMaterialized extends MaterializationStrategy("NOT MATERIALIZED")

case class DexPkfkMaterializationAwareJoin(predicate: String, labelColumnOrder: Attribute, leftTableRid: Attribute, leftMs: MaterializationStrategy, rightMs: MaterializationStrategy, left: LogicalPlan, right: LogicalPlan, output: Seq[Attribute]) extends BinaryNode {
  override def references: AttributeSet = super.references ++ AttributeSet(output)
}

// output: childView's schema plus
case class DexPkfkDependentJoin(predicate: String, labelColumn: String, labelColumnOrder: Attribute, leftTable: LogicalPlan, leftTableName: String, leftTableRid: Attribute, rightTable: LogicalPlan, rightTableName: String, rightTableRid: Attribute) extends BinaryNode {
  override def left: LogicalPlan = leftTable // only for resolving leftTableRid

  override def right: LogicalPlan = rightTable // only for resolving labelColumn

  override def output: Seq[Attribute] = left.output ++ right.output // Seq(leftTableRid, rightTableRid)

  override def references: AttributeSet = super.references ++ AttributeSet(output)
}

case class DexPkfkIndepJoin(predicate: String, labelColumn: String, labelColumnOrder: Attribute, leftTable: LogicalPlan, leftTableName: String, leftTableRid: Attribute, rightTable: LogicalPlan, rightTableName: String, rightTableRid: Attribute) extends BinaryNode {
  override def left: LogicalPlan = leftTable // only for resolving leftTableRid

  override def right: LogicalPlan = rightTable // only for resolving labelColumn

  override def output: Seq[Attribute] = Seq(leftTableRid, rightTableRid)

  override def references: AttributeSet = super.references ++ AttributeSet(output)
}

case class DexPkfkRightTableJoin(predicate: String, labelColumn: String, labelColumnOrder: Attribute, leftTable: LogicalPlan, leftTableName: String, leftTableRid: Attribute, rightTable: LogicalPlan, rightTableName: String, rightTableRid: Attribute) extends BinaryNode {
  override def left: LogicalPlan = leftTable // only for resolving leftTableRid

  override def right: LogicalPlan = rightTable // only for resolving labelColumn

  override def output: Seq[Attribute] = leftTableRid +: right.output

  override def references: AttributeSet = super.references ++ AttributeSet(output)
}

case class DexPkfkLeftDependentJoin(predicate: String, labelColumn: String, labelColumnOrder: Attribute, leftTable: LogicalPlan, leftTableName: String, leftTableRid: Attribute, rightTable: LogicalPlan, rightTableName: String, rightTableRid: Attribute) extends BinaryNode {
  override def left: LogicalPlan = leftTable // only for resolving leftTableRid

  override def right: LogicalPlan = rightTable // only for resolving labelColumn

  override def output: Seq[Attribute] = left.output :+ rightTableRid

  override def references: AttributeSet = super.references ++ AttributeSet(output)
}