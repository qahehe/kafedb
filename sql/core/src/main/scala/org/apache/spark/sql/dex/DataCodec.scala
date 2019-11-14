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


object DataCodec {

  val charsetName: Map[String, String] = Map("scala" -> "ISO-8859-1", "postgres" -> "iso_8859_1")

  def toByteArray(value: Any): Seq[Byte] = value match {
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
    case x: String => x.getBytes(charsetName("scala"))
    case x: java.sql.Date => toByteArray(x.getTime)
    case x: java.sql.Time => toByteArray(x.getTime)
    case _ => throw DexException("unsupported")
  }

  def asString(value: Seq[Byte]): String = new String(value.toArray, "ISO-8859-1")

  def asHexString(value: Seq[Byte]): String = DatatypeConverter.printHexBinary(value.toArray)

  def fromHexString(value: String): Seq[Byte] = DatatypeConverter.parseHexBinary(value)

  def toDouble(value: Seq[Byte]): Double = ByteBuffer.wrap(value.toArray).getDouble

  def toInt(value: Seq[Byte]): Int = ByteBuffer.wrap(value.toArray).getInt

  def toLong(value: Seq[Byte]): Long = ByteBuffer.wrap(value.toArray).getLong

  def withoutInitialValue(iv: Array[Byte], value: Array[Byte]): Seq[Byte] = util.Arrays.copyOfRange(value, iv.length, value.length)

  def concat(xs: Any*): Seq[Byte] = {
    val ys = xs.map(x => toByteArray(x))
    val buf = ByteStreams.newDataOutput(ys.map(_.length).sum)
    ys.foreach(y => buf.write(y.toArray))
    buf.toByteArray
  }
}
