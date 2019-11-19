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

import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec
import org.apache.spark.sql.catalyst.dex.DexConstants.TableAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Concat, DialectSQLTranslatable, Literal}
import org.apache.spark.sql.types.AtomicType
import org.bouncycastle.util.encoders.Hex

object DexPrimitives {

  val masterSecret: Crypto.MasterSecret = Crypto.getPseudoMasterSecret

  def dexTableNameOf(tableName: String): String =
    "t" + Hex.toHexString(Crypto.prf(masterSecret.hmacKey, tableName))

  def dexColNameOf(colName: String): String =
    "c" + Hex.toHexString(Crypto.prf(masterSecret.hmacKey, colName))

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

  def dexEmmValueOf(dexPredicate: String, value: Long): Array[Byte] = {
    //Crypto.symEnc(dexEmmValueEncKeyOf(dexPredicate))(s"$value")
    val trapdoor = dexEmmValueEncKeyOf(dexPredicate)
    Crypto.symEnc(trapdoor, value)
  }

  def dexEmmLabelPrfKeyOf(dexPredicate: String): String = dexPredicate // todo: append 1 and apply F
  def dexEmmValueEncKeyOf(dexPredicate: String): SecretKey = {
    val trapdoorBytes = Crypto.prf(masterSecret.hmacKey, dexPredicate)
    val trapdoor = new SecretKeySpec(trapdoorBytes, Crypto.aesAlgorithm)
    trapdoor
  }

  def dexCellOf(cell: Any): Array[Byte] = {
    Crypto.symEnc(masterSecret.aesKey, cell)
  }

  def dexRidOf(rid: Long): Long = {
    //Crypto.prf(masterSecret.hmacKey, rid)
    rid
  }

  def catalystDecryptAttribute(attr: Attribute): DexDecrypt = {
      DexDecrypt(Literal(masterSecret.aesKey.getEncoded), attr)
  }

  def catalystEmmLabelPrfKeyExprOf(predicateExpr: DialectSQLTranslatable): DialectSQLTranslatable  = {
    // todo: use Postgres PRF on key=client secret key append 1
    predicateExpr
  }

  def catalystEmmValueEncKeyExprOf(predicateExpr: DialectSQLTranslatable): DexPrf = {
    DexPrf(Literal(masterSecret.hmacKey.getEncoded), predicateExpr)
  }

  def catalystEmmLabelExprOf(dbEmmLabelPrfKeyExpr: DialectSQLTranslatable, counterExpr: DialectSQLTranslatable): DialectSQLTranslatable  = {
    // todo: instead of concat, use Postgres PRF on key=dbEmmLabelprfKeyExpr
    //s"$dbEmmLabelPrfKeyExpr || '~' || $counterExpr"
    Concat(dbEmmLabelPrfKeyExpr :: Literal("~") :: counterExpr :: Nil)
  }

  def catalystConcatPredicateExprsOf(predicatePrefixExpr: DialectSQLTranslatable, predicateExpr: DialectSQLTranslatable): DialectSQLTranslatable = {
    //s"$predicatePrefixExpr || '~' || $predicateExpr"
    Concat(predicatePrefixExpr :: Literal("~") :: predicateExpr :: Nil)
  }

  //
  // Used by translation
  //


  def sqlDecryptExpr(encKeyExpr: String, colExpr: String): String = {
    // todo: use SQL decrypt like s"decrypt($decKey, $col)"
    //s"substring($colExpr, '(.+)_' || $encKeyExpr)"
    val ivExpr = s"substring($colExpr, 1, ${Crypto.aesBlockByteSize})"
    val dataExpr = s"substring($colExpr, ${Crypto.aesBlockByteSize} + 1, octet_length($colExpr) - ${Crypto.aesBlockByteSize})"
    s"decrypt_iv($dataExpr, $encKeyExpr, $ivExpr, 'aes-cbc/pad:pkcs')"
  }

  def sqlPrfExpr(keyExpr: String, colExpr: String): String = {
    s"hmac($colExpr, $keyExpr, 'sha256')"
  }
}
