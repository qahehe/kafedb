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
package org.apache.spark.sql.catalyst.dex
// scalastyle:off

import org.apache.spark.sql.catalyst.dex.DexConstants.{TableAttribute, TableName}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Concat, DialectSQLTranslatable, Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, IntegralType, LongType}
import org.bouncycastle.util.encoders.Hex

object DexPrimitives {

  val masterSecret: Crypto.MasterSecret = Crypto.getPseudoMasterSecret

  def dexTableNameOf(tableName: String): String =
    "t" + tableName
    // "t" + Hex.toHexString(Crypto.prf(masterSecret.hmacKey, DataCodec.encode(tableName)))

  def dexColNameOf(colName: String): String =
    "c" + colName
    // "c" + Hex.toHexString(Crypto.prf(masterSecret.hmacKey, DataCodec.encode(colName)))

  def dexPkfkJoinPredicatePrefixOf(tableLeft: TableName, tableRight: TableName): String = {
    s"${tableLeft}~${tableRight}"
  }

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

  def dexFilterPredicate(predicatePrefix: String)(value: Any): String = {
    s"$predicatePrefix~${value.toString}"
  }

  def dexMasterTrapdoorForPred(dexPredicate: String, index: Option[Int]): Array[Byte] = {
    index.map(
      i => dexTrapdoorForPred(DexPrimitives.masterSecret.hmacKey.getEncoded, dexPredicate, i)
    ).getOrElse(
      dexTrapdoorForPred(DexPrimitives.masterSecret.hmacKey.getEncoded, dexPredicate)
    )
  }

  def dexSecondaryTrapdoorForRid(masterTrapdoor: Array[Byte], rid: Long, index: Option[Int]): Array[Byte] = {
    index.map(
      i => dexTrapdoorForRid(masterTrapdoor, rid, i)
    ).getOrElse(
      dexTrapdoorForRid(masterTrapdoor, rid)
    )
  }

  def dexTrapdoorForPred(key: Array[Byte], predicate: String): Array[Byte] = {
    Crypto.prf(key, DataCodec.encode(predicate))
  }

  def dexTrapdoorForRid(key: Array[Byte], rid: Long): Array[Byte] = {
    Crypto.prf(key, DataCodec.concatBytes(dexRidOf(rid)))
  }

  def dexTrapdoorForPred(key: Array[Byte], predicate: String, j: Int): Array[Byte] = {
    Crypto.prf(key, DataCodec.concatBytes(predicate, j))
  }

  def dexTrapdoorForRid(key: Array[Byte], rid: Long, j: Int): Array[Byte] = {
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

  def catalystTrapdoorExprOf(key: Expression, predBytesExpr: DialectSQLTranslatable): DialectSQLTranslatable = {
    // DexPrf(key, DexEncode(predicateExpr, LongType))
    DexPrf(key, DexEncode(predBytesExpr, BinaryType))
  }

  def catalystTrapdoorExprOf(key: Expression, predBytesExpr: DialectSQLTranslatable, j: Int): DialectSQLTranslatable = {
    DexPrf(key, Concat(DexEncode(predBytesExpr, BinaryType) :: DexEncode(Literal(j), IntegerType) :: Nil))
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
