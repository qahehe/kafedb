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

import java.nio.ByteBuffer
import java.util

import com.google.common.io.ByteStreams
import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.types.{AtomicType, DataType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeOf

object DataCodec {

  val charsetName: Map[String, String] = Map("scala" -> "UTF-8", "postgres" -> "UTF8")

  @scala.annotation.tailrec
  def encode(value: Any): Array[Byte] = value match {
    case x: Int =>
      val buf = new Array[Byte](java.lang.Integer.BYTES)
      ByteBuffer.wrap(buf).putInt(x)
      buf
    case x: Long =>
      val buf = new Array[Byte](java.lang.Long.BYTES)
      ByteBuffer.wrap(buf).putLong(x)
      buf
    case x: java.math.BigDecimal =>
      x.toString.getBytes(charsetName("scala"))
    case x: Double =>
      val buf = new Array[Byte](java.lang.Double.BYTES)
      ByteBuffer.wrap(buf).putDouble(x)
      buf
    case x: Seq[Byte @unchecked] => x.toArray
    case x: Array[Byte] => x
    case x: String => x.getBytes(charsetName("scala"))
    case x: java.sql.Date => encode(x.getTime)
    case x: java.sql.Time => encode(x.getTime)
    case x => throw DexException("unsupported: " + x.getClass.getName)
  }

  //def decodeString(value: Array[Byte]): String = new String(value.toArray, charsetName("scala"))

  def decode[T: TypeTag](value: Array[Byte]): T = {
    val v = typeOf[T] match {
      case t if t =:= typeOf[String] => new String(value, charsetName("scala"))
      case t if t =:= typeOf[Int] => ByteBuffer.wrap(value).getInt
      case t if t =:= typeOf[Long] => ByteBuffer.wrap(value).getLong
      case t if t =:= typeOf[Double] => ByteBuffer.wrap(value).getDouble
      case t if t =:= typeOf[UTF8String] => UTF8String.fromBytes(value)
      case t if t =:= typeOf[java.math.BigDecimal] =>
        BigDecimal(new String(value, charsetName("scala")))
      case t => throw DexException("unsupported: " + t.getClass.getName)
    }
    v.asInstanceOf[T]
  }

  //def decodeDouble(value: Seq[Byte]): Double = ByteBuffer.wrap(value.toArray).getDouble

  //def decodeInt(value: Seq[Byte]): Int = ByteBuffer.wrap(value.toArray).getInt

  //def decodeLong(value: Seq[Byte]): Long = ByteBuffer.wrap(value.toArray).getLong

  def asHexString(value: Seq[Byte]): String = DatatypeConverter.printHexBinary(value.toArray)

  def fromHexString(value: String): Seq[Byte] = DatatypeConverter.parseHexBinary(value)

  def withoutInitialValue(iv: Array[Byte], value: Array[Byte]): Seq[Byte] = util.Arrays.copyOfRange(value, iv.length, value.length)

  def concat(xs: Any*): Seq[Byte] = {
    concatBytes(xs:_*)
  }

  def concatBytes(xs: Any*): Array[Byte] = {
    val ys = xs.map(x => encode(x))
    val buf = ByteStreams.newDataOutput(ys.map(_.length).sum)
    ys.foreach(y => buf.write(y))
    buf.toByteArray
  }
}
