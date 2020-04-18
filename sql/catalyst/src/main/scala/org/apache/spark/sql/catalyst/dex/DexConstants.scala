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
// scalastyle:off

object DexConstants {
  val cashCounterStart: Long = 0L
  val tFilterName = "t_filter"
  val tDepFilterName = "t_depfilter"
  val tCorrJoinName = "t_correlated_join"
  val tUncorrJoinName = "t_uncorrelated_join"
  val tDomainName = "t_domain"
  val emmLabelCol = "label"
  val emmValueCol = "value"
  val emmValueDecKeyCol = "value_dec_key"
  val ridCol = "rid"
  val tDepFilterCol = "label"

  type TableName = String
  type AttrName = String
  type JoinableAttrs = (TableAttribute, TableAttribute)

  sealed trait TableAttribute {
    def table: TableName
    def attr: AttrName
    def qualifiedName: String = table + "." + attr
  }
  case class TableAttributeAtom(table: TableName, attr: AttrName) extends TableAttribute
  case class TableAttributeCompound(table: TableName, attrs: Seq[AttrName]) extends TableAttribute {
    override def attr: AttrName = attrs.mkString("_and_")
  }
}
