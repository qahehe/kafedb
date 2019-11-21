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

import org.apache.spark.sql.catalyst.dex.DexConstants.TableAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Concat, DialectSQLTranslatable, Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, IntegralType, LongType}
import org.bouncycastle.util.encoders.Hex

object DexPrimitives {

  val masterSecret: Crypto.MasterSecret = Crypto.getPseudoMasterSecret

  def dexTableNameOf(tableName: String): String =
    "t" + Hex.toHexString(Crypto.prf(masterSecret.hmacKey, DataCodec.encode(tableName)))

  def dexColNameOf(colName: String): String =
    "c" + Hex.toHexString(Crypto.prf(masterSecret.hmacKey, DataCodec.encode(colName)))

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

  def dexTrapdoor(key: Array[Byte], predicate: String): Array[Byte] = {
    Crypto.prf(key, DataCodec.encode(predicate))
  }

  def dexTrapdoor(key: Array[Byte], predicate: String, j: Int): Array[Byte] = {
    Crypto.prf(key, DataCodec.concatBytes(predicate, j))
  }

  def dexTrapdoor(key: Array[Byte], rid: Long, j: Int): Array[Byte] = {
    Crypto.prf(key, DataCodec.concatBytes(dexRidOf(rid), j))
  }

  def dexEmmLabelOf(trapdoor: Array[Byte], counter: Long): Array[Byte] = {
    Crypto.prf(trapdoor, DataCodec.encode(counter))
  }

  def dexEmmValueOf(trapdoor: Array[Byte], rid: Long): Array[Byte] = {
    Crypto.symEnc(trapdoor, dexRidOf(rid))
  }

  def dexCellOf(cell: Any): Array[Byte] = {
    Crypto.symEnc(masterSecret.aesKey, DataCodec.encode(cell))
  }

  def dexRidOf(rid: Long): Array[Byte] = {
    // Postgres cannot convert anything to binary except for string
    // So for rids, convert them to binaries.
    DataCodec.encode(rid)
  }

  def catalystDecryptAttribute(attr: Attribute): DexDecrypt = {
    DexDecrypt(Literal(masterSecret.aesKey.getEncoded), attr)
  }

  def catalystTrapdoorExprOf(key: Expression, predicateExpr: DialectSQLTranslatable): DialectSQLTranslatable = {
    DexPrf(key, DexEncode(predicateExpr, BinaryType))
  }

  def catalystTrapdoorExprOf(key: Expression, predicateExpr: DialectSQLTranslatable, j: Int): DialectSQLTranslatable = {
    DexPrf(key, Concat(DexEncode(predicateExpr, BinaryType) :: DexEncode(Literal(j), IntegerType) :: Nil))
  }

  def catalystEmmLabelExprOf(trapdoorExpr: DialectSQLTranslatable, counterExpr: DialectSQLTranslatable): DialectSQLTranslatable  = {
    // todo: instead of concat, use Postgres PRF on key=dbEmmLabelprfKeyExpr
    //s"$dbEmmLabelPrfKeyExpr || '~' || $counterExpr"
    //Concat(dbEmmLabelPrfKeyExpr :: Literal("~") :: counterExpr :: Nil)
    DexPrf(trapdoorExpr, DexEncode(counterExpr, LongType))
  }

  //
  // Used by translation
  //

  def sqlDecryptExpr(encKeyExpr: String, colExpr: String): String = {
    // Need to add conversion to binary for each expression, especially inside substring and octet_length functions.
    // Otherwise these functions treat the arguments as strings rather than binaries.
    val ivExpr = s"substring($colExpr::bytea, 1, ${Crypto.aesBlockByteSize})"
    val dataExpr = s"substring($colExpr::bytea, ${Crypto.aesBlockByteSize + 1}, octet_length($colExpr::bytea) - ${Crypto.aesBlockByteSize})"
    s"decrypt_iv($dataExpr::bytea, $encKeyExpr::bytea, $ivExpr::bytea, 'aes-cbc/pad:pkcs')"
  }

  def sqlPrfExpr(keyExpr: String, colExpr: String): String = {
    s"hmac($colExpr, $keyExpr, 'sha256')"
  }

  def sqlEncodeExpr(binaryExpr: String): String = {
    s"$binaryExpr::bytea"
  }

  def sqlEncodeExpr(expr: String, exprType: DataType): String = {
    exprType match {
      case IntegerType => s"int4send($expr)"
      case LongType => s"int8send($expr)"
      case BinaryType => expr
      case z => throw DexException("unsupported: " + z.toString)
    }
  }
}
