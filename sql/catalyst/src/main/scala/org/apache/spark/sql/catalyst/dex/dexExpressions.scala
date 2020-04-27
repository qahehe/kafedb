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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, DialectSQLTranslatable, ExpectsInputTypes, Expression, SqlDialect, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, AtomicType, BinaryType, DataType, IntegerType, IntegralType, LongType, StringType}

import scala.reflect.runtime.universe.TypeTag

case class DexDecrypt(key: Expression, value: Expression) extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  override def left: Expression = key
  override def right: Expression = value
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)
  override def dataType: DataType = BinaryType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    //val fromCharset = input2.asInstanceOf[UTF8String].toString
    //UTF8String.fromString(new String(input1.asInstanceOf[Array[Byte]], fromCharset))
    //UTF8String.fromString("""(.+)_enc""".r.findFirstMatchIn(input2.asInstanceOf[UTF8String].toString).get.group(1))
    // todo: avoid constructing the key all the time.  Pass in SecretKey instead of key bytes.
    val keyInput = Crypto.aesKeyFrom(input1.asInstanceOf[Array[Byte]])
    val valueInput = input2.asInstanceOf[Array[Byte]]
    Crypto.symDec(keyInput, valueInput)
  }

  /*override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (key, value) =>
      s"""
          ${ev.value} = $value.split(UTF8String.fromString("_enc"), 0)[0];
      """)
  }*/

  override protected def dialectSqlExpr(dialect: SqlDialect): String = {
    DexPrimitives.sqlDecryptExpr(
      left.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect),
      right.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect))
  }
}

case class DexPrf(key: Expression, data: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {
  override def left: Expression = key
  override def right: Expression = data
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)
  override def dataType: DataType = BinaryType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ???

  protected override def dialectSqlExpr(dialect: SqlDialect): String = {
    DexPrimitives.sqlPrfExpr(
      key.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect),
      right.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect)
    )
  }
}

case class DexDecode(bin: Expression, decodeType: AtomicType)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {
  override def child: Expression = bin
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = decodeType

  protected override def nullSafeEval(input: Any): Any = {
    val inputBytes = input.asInstanceOf[Array[Byte]]
    val res = DataCodec.decode(inputBytes)(decodeType.tag)
    res
  }
}

case class DexEncode(expr: Expression, exprType: DataType) // exprType is input type, not output type
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {
  override def child: Expression = expr
  override def inputTypes: Seq[AbstractDataType] = Seq(exprType)
  override def dataType: DataType = BinaryType

  protected override def nullSafeEval(input: Any): Any = {
    DataCodec.encode(input)
  }

  protected override def dialectSqlExpr(dialect: SqlDialect): String = {
      DexPrimitives.sqlEncodeExpr(expr.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect), exprType)
  }
}