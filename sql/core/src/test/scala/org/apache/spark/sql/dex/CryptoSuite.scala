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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dex.{Crypto, DataCodec, DexDecrypt, DexPrf, DexPrimitives}
import org.bouncycastle.util.encoders.Hex

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.expressions.Literal

// scalastyle:off

class CryptoSuite extends DexTest {

  test("save and load master secret") {
    val masterSecret = Crypto.generateMasterSecret()
    val filePath = "/tmp/dexkeystore"
    Crypto.saveMasterSecret("testPass", masterSecret, filePath)
    val masterSecretLoaded = Crypto.loadMasterSecret("testPass", filePath)
    assert(masterSecret == masterSecretLoaded)
  }

  test("symmetric encryption and decryption in scala") {
    def testForTyped[T: TypeTag](message: T) = {
      val masterSecret = Crypto.generateMasterSecret()
      val ciphertext = Crypto.symEnc(masterSecret.aesKey, DataCodec.encode(message))
      val decBytes = Crypto.symDec(masterSecret.aesKey, ciphertext)
      val decMessage: T = DataCodec.decode(decBytes)
      assert(message == decMessage)
    }
    testForTyped("abcdef")
    testForTyped(123)
    testForTyped(123L)
    testForTyped(1.23)
  }

  test("symmetric encryption in catalyst") {
    def testForTyped[T: TypeTag](message: T) = {
      val masterSecret = Crypto.generateMasterSecret()
      val ciphertext = Crypto.symEnc(masterSecret.aesKey, DataCodec.encode(message))
      val decExpr = DexDecrypt(Literal(masterSecret.aesKey.getEncoded), Literal(ciphertext))
      val decBytes = decExpr.eval(InternalRow.empty).asInstanceOf[Array[Byte]]
      val decMessage: T = DataCodec.decode(decBytes)
      println(decBytes.length)
      assert(message == decMessage)
    }
    testForTyped("abcdef")
    testForTyped(123)
    testForTyped(123L)
    testForTyped(1.23)
  }

  test("concatenate rid with index in dex and postgres for rid") {
    val rid = 10L
    //val dexTrapdoor = DexPrimitives.dexTrapdoor(DexPrimitives.masterSecret.hmacKey.getEncoded, 10, 2)
    val j = 2
    val dexTrapdoor = DataCodec.concatBytes(DexPrimitives.dexRidOf(rid), DataCodec.encode(j.toString))
    val postgresTrapdoor = {
      val rs = connEnc.prepareStatement(s"select ${Literal(DexPrimitives.dexRidOf(rid)).dialectSql(dialect)} || $j::text::bytea").executeQuery
      assert(rs.next())
      rs.getObject(1).asInstanceOf[Array[Byte]]
    }
    assert(dexTrapdoor === postgresTrapdoor)
  }

  test("hmac in dex and postgres only equals for string") {
    val masterSecret = Crypto.generateMasterSecret()
    val data = 10.toString
    val dexHash = Crypto.prf(masterSecret.hmacKey, DataCodec.encode(data))
    val postgresHash = {
      val rs = connEnc.prepareStatement(s"select hmac($data::text::bytea, ${Literal(masterSecret.hmacKey.getEncoded).dialectSql(dialect)}, 'sha256')").executeQuery
      assert(rs.next())
      rs.getObject(1).asInstanceOf[Array[Byte]]
    }
    assert(dexHash === postgresHash)
  }

  test("aes in dex and postgres equals for bytes") {
    val masterSecret = Crypto.getPseudoMasterSecret
    val data = DataCodec.encode(123L)
    val dexCiphertext = Crypto.symEnc(masterSecret.aesKey, data)
    println(dexCiphertext.length)
    println(Hex.toHexString(dexCiphertext))
    println(Literal(dexCiphertext).dialectSql(dialect))
    println(Hex.toHexString(masterSecret.aesKey.getEncoded))
    println(Literal(masterSecret.aesKey.getEncoded).dialectSql(dialect))

    val dataPostgres = {
      val decFunPg = DexPrimitives.sqlDecryptExpr(Literal(masterSecret.aesKey.getEncoded).dialectSql(dialect), Literal(dexCiphertext).dialectSql(dialect))
      val rs = connEnc.prepareStatement(s"select $decFunPg").executeQuery()
      assert(rs.next())
      rs.getObject(1).asInstanceOf[Array[Byte]]
    }
    assert(data === dataPostgres)
  }

  test("value encode byte length") {
    def testForTyped[T: TypeTag](value: T): Unit = {
      println(DataCodec.encode(value).length)
    }
    testForTyped("12345")
    testForTyped(12345)
    testForTyped(12345L)
    testForTyped(1.2345)
  }

  test("aes cbc example ") {
    Crypto.example()
  }

  test("hex decode") {
    val keyBytes = Hex.decode("000102030405060708090a0b0c0d0e0f")
    println(keyBytes.length)
  }

  test("dex table name") {
    val dexTableName = DexPrimitives.dexTableNameOf("foo")
    println(dexTableName)
    println(dexTableName.length)
  }

  test("dex column name") {
    val dexColName = DexPrimitives.dexColNameOf("col1")
    println(dexColName)
    println(dexColName.length)
  }

  test("dex rid") {
    println(DexPrimitives.dexRidOf(123L))
  }
}