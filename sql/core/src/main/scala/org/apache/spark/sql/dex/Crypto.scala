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

import org.apache.spark.sql.dex.DexConstants.TableAttribute
// scalastyle:off

object Crypto {
  def prf(k: String)(m: Any): String = {
    s"${m}_$k"
  }

  def symEnc(k: String)(m: Any): String = {
    s"${m}_$k"
  }
}

object DexPrimitives {

  val prfKey = "prf"
  val symEncKey = "enc"

  def dexTableNameOf(tableName: String): String = Crypto.prf(prfKey)(tableName)

  def dexColNameOf(colName: String): String = Crypto.prf(prfKey)(colName)

  def dexCorrJoinPredicatePrefixOf(attrLeft: TableAttribute, attrRight: TableAttribute): String = {
    s"${attrLeft.table}~${attrLeft.attr}~${attrRight.table}~${attrRight.attr}"
  }

  def dexUncorrJoinPredicateOf(attrLeft: TableAttribute, attrRight: TableAttribute): String = {
    s"${attrLeft.table}~${attrLeft.attr}~${attrRight.table}~${attrRight.attr}"
  }

  def dexFilterPredicatePrefixOf(table: String, column: String): String = {
    s"$table~$column"
  }

  def dexDomainPredicateOf(table: String, column: String): String = {
    s"$table~$column"
  }

  def dexPkFKJoinPredicateOf(leftTableAttr: TableAttribute, rightTableAttr: TableAttribute): String = {
    s"${leftTableAttr.table}~${rightTableAttr.table}"
  }

  def dexPredicatesConcat(predicatePrefix: String)(predicateTerm: String): String = {
    s"$predicatePrefix~$predicateTerm"
  }

  def dexEmmLabelOf(dexPredicate: String, counter: Int): String = {
    // todo: use dexEmmLabelPrfKey(dexPredicate) for k, return F_k(counter)
    s"${dexEmmLabelPrfKeyOf(dexPredicate)}~$counter"
  }

  def dexEmmValueOf(dexPredicate: String, value: Number): String = {
    Crypto.symEnc(dexEmmValueEncKeyOf(dexPredicate))(s"$value")
  }

  def dexCellOf(cell: String): String = {
    Crypto.symEnc(symEncKey)(cell)
  }

  def dexRidOf(rid: Number): String = {
    Crypto.prf(prfKey)(rid)
  }

  def dexEmmLabelPrfKeyOf(dexPredicate: String): String = dexPredicate // todo: append 1 and apply F
  def dexEmmValueEncKeyOf(dexpredicate: String): String = "enc" // todo: append 2 and apply F

  def dbEmmLabelPrfKeyExprOf(predicateExpr: String): String = {
    // todo: use Postgres PRF on key=client secret key append 1
    predicateExpr
  }

  def dbEmmValueEncKeyExprOf(predicateExpr: String): String = {
    // todo: use Postgres PRF on key=client secret key append 2
    "'enc'"
  }

  def dbEmmLabelExprOf(dbEmmLabelPrfKeyExpr: String, counterExpr: String): String = {
    // todo: use Postgres PRF on key=dbEmmLabelprfKeyExpr
    s"$dbEmmLabelPrfKeyExpr || '~' || $counterExpr"
  }

  def dbConcatPredicateExprsOf(predicatePrefixExpr: String, predicateExpr: String): String = {
    s"$predicatePrefixExpr || '~' || $predicateExpr"
  }

  def dbDecryptExpr(encKeyExpr: String, colExpr: String): String =
  // todo: use SQL decrypt like s"decrypt($decKey, $col)"
    s"substring($colExpr, '(.+)_' || $encKeyExpr)"
}
