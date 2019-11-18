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
// scalastyle:off

import java.nio.ByteBuffer
import java.util

import com.google.common.io.ByteStreams
import javax.xml.bind.DatatypeConverter

import scala.reflect.runtime.universe._

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
    case x: Double =>
      val buf = new Array[Byte](java.lang.Double.BYTES)
      ByteBuffer.wrap(buf).putDouble(x)
      buf
    case x: Seq[Byte @unchecked] => x.toArray
    case x: Array[Byte] => x
    case x: String => x.getBytes(charsetName("scala"))
    case x: java.sql.Date => encode(x.getTime)
    case x: java.sql.Time => encode(x.getTime)
    case _ => throw DexException("unsupported")
  }

  //def decodeString(value: Array[Byte]): String = new String(value.toArray, charsetName("scala"))

  def decode[T: TypeTag](value: Array[Byte]): T = {
    val v = typeOf[T] match {
      case t if t =:= typeOf[String] => new String(value, charsetName("scala"))
      case t if t =:= typeOf[Int] => ByteBuffer.wrap(value).getInt
      case t if t =:= typeOf[Long] => ByteBuffer.wrap(value).getLong
      case t if t =:= typeOf[Double] => ByteBuffer.wrap(value).getDouble
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
