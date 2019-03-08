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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types.IntegerType

class Encrypter(catalog: SessionCatalog) extends RuleExecutor[LogicalPlan] {

  lazy val batches: Seq[Batch] = Seq(
    Batch("Encrypt Constant Filter", Once, EncryptConstantFilter),
    Batch("Encrypt Projection", Once, EncryptProjection)
  )

}

object EncryptConstantFilter extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case f @ Filter(filterCondition, child: SubqueryAlias) =>
      val childNameEnc = s"${child.alias}_enc"
      val childEnc = table(childNameEnc)
      constantFilterRidsOf(filterCondition, child.alias).join(childEnc, joinType = LeftOuter,
        condition = Some(EqualTo(
          UnresolvedAttribute("dvalue"),
          UnresolvedAttribute(s"${childNameEnc}.rid"))))
  }

  def constantFilterRidsOf(filterCondition: Expression, tableName: String): LogicalPlan =
    filterCondition match {
      case EqualTo(left: AttributeReference, right @ Literal(value, IntegerType)) =>
        val colName = left.name
        table(s"tselect_${tableName}_$colName")
          .select("value".as("dvalue")).where(EqualTo($"rid", s"${value.asInstanceOf[Int]}~counter")) // .subquery('const_filter_rids)
      case _ => throw EncryptionException("unsupported")
    }
}

object EncryptProjection extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    // fixme: maybe too broad of a match, only match ridmatrix join
    case p @ Project(projectList, child: Join) =>
      val encProjectList = projectList.map {
        case x: AttributeReference => encAttrOf(x)
        case a: Alias =>
          val encAliasChild = a.child match {
            case c: Cast =>
              val encCastChild = c.child match {
                case x: AttributeReference => encAttrOf(x)
              }
              c.copy(child = encCastChild)
          }
          a.copy(child = encAliasChild)(
            exprId = a.exprId, qualifier = a.qualifier, explicitMetadata = a.explicitMetadata)
      }
      p.copy(projectList = encProjectList)
  }

  def encAttrOf(attr: AttributeReference): UnresolvedAttribute = $"${attr.name}_enc"
}

