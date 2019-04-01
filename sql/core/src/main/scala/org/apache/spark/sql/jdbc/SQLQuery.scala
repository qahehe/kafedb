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

package org.apache.spark.sql.jdbc

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation

trait SQLView

case class SQLQuery(project: Seq[NamedExpression],
                    join: Seq[Expression],
                    filter: Seq[Expression],
                    views: Seq[SQLView]) extends SQLView {

  def makeCopy(project: Seq[NamedExpression] = project,
               join: Seq[Expression] = join,
               filter: Seq[Expression] = filter,
               views: Seq[SQLView] = views): SQLQuery =
    SQLQuery(project, join, filter, views)
}

case class SQLTable(tableName: String) extends SQLView

object SQLQuery {
  def fromQueryPlan(p: LogicalPlan): SQLQuery = {
    fromQueryPlanRecursive(p, SQLQuery(Seq.empty, Seq.empty, Seq.empty, Seq.empty))
  }

  private def fromQueryPlanRecursive(plan: LogicalPlan, query: SQLQuery): SQLQuery = {
    plan match {
      case p: Project => query.makeCopy(project = query.project ++ p.projectList)

      case p: Filter =>
        // todo: IN query
        query.makeCopy(filter = query.filter :+ p.condition)

      case p: Join =>
        val queryLeft = fromQueryPlanRecursive(p.left, query)
        val queryRight = fromQueryPlanRecursive(p.right, queryLeft)
        queryRight.makeCopy(join = queryRight.join ++ p.condition.toList)

      case p: LogicalRelation => p.relation match {
        case r: JDBCRelation =>
          query.makeCopy(views = query.views :+ SQLTable(r.jdbcOptions.tableOrQuery))
        case x => throw SQLQueryError("unsupported: " + x.getClass.getName)
      }

      case x => throw SQLQueryError("unsupported: " + x.getClass.getName)
    }
  }
}

case class SQLQueryError(msg: String) extends RuntimeException(msg)
