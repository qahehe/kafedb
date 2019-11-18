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

import org.bouncycastle.util.encoders.Hex

import scala.reflect.runtime.universe._

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
      val ciphertext = Crypto.symEnc(masterSecret.aesKey, message)
      val decBytes = Crypto.symDec(masterSecret.aesKey, ciphertext)
      val decMessage: T = DataCodec.decode(decBytes)
      assert(message == decMessage)
    }
    testForTyped("abcdef")
    testForTyped(123)
    testForTyped(123L)
    testForTyped(1.23)
  }

  test("value encode byte length") {
    def testForTyped[T: TypeTag](value: T) = {
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
}