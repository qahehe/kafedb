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
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, StringType)
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

case class DexEncodeNumberString(expr: Expression, numType: IntegralType)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {
  override def child: Expression = expr
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)
  override def dataType: DataType = BinaryType

  protected override def nullSafeEval(input: Any): Any = {
    DataCodec.encode(input.toString)
  }

  protected override def dialectSqlExpr(dialect: SqlDialect): String = {
    DexPrimitives.sqlEncodeNumberStringExpr(expr.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect))
  }
}