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

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.security.SecureRandom

import javax.crypto.{Cipher, KeyGenerator, Mac, SecretKey, SecretKeyFactory}
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, PBEParameterSpec, SecretKeySpec}
import org.bouncycastle.util.encoders.Hex

object Crypto {
  // dynamic installation of bouncy castle provider
  java.security.Security.addProvider(
    new org.bouncycastle.jce.provider.BouncyCastleProvider())

/*  def prf(k: String)(m: Any): String = {
    s"${m}_$k"
  }

  def symEnc(k: String)(m: Any): String = {
    s"${m}_$k"
  }*/

  val aesBlockByteSize: Int =
    Cipher.getInstance("AES/CBC/PKCS7Padding", "BC").getBlockSize

  def prf(key: SecretKey, m: Array[Byte]): Array[Byte] = {
    //s"${m}_$k"
    computeHmac(key, m)
  }
  def prf(keyBytes: Array[Byte], m: Array[Byte]): Array[Byte] = {
    prf(new SecretKeySpec(keyBytes, hmacAlgorithm), m)
  }

  def symEnc(key: SecretKey, m: Array[Byte]): Array[Byte] = {
    //s"${m}_$k"
    encryptAesCbc(key, m)
  }
  def symEnc(keyBytes: Array[Byte], m: Array[Byte]): Array[Byte] = {
    symEnc(new SecretKeySpec(keyBytes, aesAlgorithm), m)
  }

  def symDec(key: SecretKey, c: Array[Byte]): Array[Byte] = {
    decryptAesCbc(key, c)
  }

  def example(): Unit = {
    val keyBytes = Hex.decode("000102030405060708090a0b0c0d0e0f")
    val  key = new SecretKeySpec(keyBytes, "AES")
    val  cipher = Cipher.getInstance("AES/CBC/PKCS7Padding", "BC")
    //val  input = Hex.decode("a0a1a2a3a4a5a6a7a0a1a2a3a4a5a6a7" + "a0a1a2a3a4a5a6a7a0a1a2a3a4a5a6a7")
    val input = DataCodec.encode("a0a1a2a3a4a5a6a7a0a1a2a3a4a5a6a7" + "a0a1a2a3a4a5a6a7a0a1a2a3a4a5a6a7")
    //System.out.println("input : " + Hex.toHexString(input))
    System.out.println("input : " + DataCodec.decode[String](input))
    cipher.init(Cipher.ENCRYPT_MODE, key)
    val iv = cipher.getIV
    val output = cipher.doFinal(input)
    System.out.println("encrypted: " + Hex.toHexString(output))
    cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv))
    //System.out.println("decrypted: " + Hex.toHexString(cipher.doFinal(output)))
    System.out.println("decrypted: " + DataCodec.decode[String](cipher.doFinal(output)))
  }

  val passphraseIterations: Int = 1 << 10 // 1024
  val aesKeyBitLength = 256
  val aesAlgorithm = "AES"
  val hmacKeyBitLength = 256
  val hmacAlgorithm = "HmacSHA256"

  private def computeHmac(hmacKey: SecretKey, data: Array[Byte]): Array[Byte] = {
    require(isHmacKey(hmacKey))
    val mac = Mac.getInstance(hmacAlgorithm, "BC")
    mac.init(hmacKey)
    mac.update(data)
    mac.doFinal()
  }

  private def encryptAesCbc(aesKey: SecretKey, data: Array[Byte]): Array[Byte] = {
    require(isAesKey(aesKey))
    val cipher = Cipher.getInstance("AES/CBC/PKCS7Padding", "BC")
    cipher.init(Cipher.ENCRYPT_MODE, aesKey)
    val iv = cipher.getIV
    val output = cipher.doFinal(data)
    DataCodec.concatBytes(iv, output)
  }

  private def decryptAesCbc(aesKey: SecretKey, ciphertext: Array[Byte]): Array[Byte] = {
    require(isAesKey(aesKey))
    val cipher = Cipher.getInstance("AES/CBC/PKCS7Padding", "BC")
    val iv = ciphertext.take(cipher.getBlockSize)
    val encData = ciphertext.drop(cipher.getBlockSize)
    cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(iv))
    cipher.doFinal(encData)
  }

  private def isAesKey(aesKey: SecretKey): Boolean = {
    aesKey.getEncoded.length == aesKeyBitLength / 8 && aesKey.getAlgorithm.toLowerCase == aesAlgorithm.toLowerCase
  }

  private def isHmacKey(hmacKey: SecretKey): Boolean = {
    hmacKey.getEncoded.length == hmacKeyBitLength / 8 && hmacKey.getAlgorithm.toLowerCase == hmacAlgorithm.toLowerCase
  }

  def aesKeyFrom(keyBytes: Array[Byte]): SecretKey = {
    require(keyBytes.length == aesKeyBitLength / 8)
    new SecretKeySpec(keyBytes, aesAlgorithm)
  }

  @SerialVersionUID(1L)
  case class MasterSecret(aesKey: SecretKey, hmacKey: SecretKey) extends Serializable {
    require(isAesKey(aesKey))
    require(isHmacKey(hmacKey))
  }

  // Do not objectize this method to singleton of MasterSecret,
  // because if so and if it is called before the Crypto, the bouncystle provider is not yet added
  // the Crypto singleton object constructor.
  def generateMasterSecret(): MasterSecret = {
    val aesKey = generateSecret(aesAlgorithm, aesKeyBitLength)
    val hmacKey = generateSecret(hmacAlgorithm, hmacKeyBitLength)
    MasterSecret(aesKey, hmacKey)
  }

  // for debugging only
  def getPseudoMasterSecret: MasterSecret = {
    val sixteenBytes = Hex.decode("000102030405060708090a0b0c0d0e0f")
    val aesKeyBytes = DataCodec.concatBytes(sixteenBytes, sixteenBytes)
    val hmacKeyBytes = DataCodec.concatBytes(sixteenBytes, sixteenBytes)
    val aesKey = new SecretKeySpec(aesKeyBytes, aesAlgorithm)
    val hmacKey = new SecretKeySpec(hmacKeyBytes, hmacAlgorithm)
    MasterSecret(aesKey, hmacKey)
  }

  private def decodeMasterSecret(masterSecretBytes: Array[Byte]): MasterSecret = {
    require(masterSecretBytes.length == aesKeyBitLength / 8 + hmacKeyBitLength / 8)
    val aesKey = new SecretKeySpec(masterSecretBytes.take(aesKeyBitLength / 8), aesAlgorithm)
    val hmacKey = new SecretKeySpec(masterSecretBytes.drop(aesKeyBitLength / 8), hmacAlgorithm)
    MasterSecret(aesKey, hmacKey)
  }

  private def encodeMasterSecret(masterSecret: MasterSecret): Array[Byte] = {
    DataCodec.concatBytes(masterSecret.aesKey.getEncoded, masterSecret.hmacKey.getEncoded)
  }

  private def generateSecret(algorithm: String, keyBitLength: Int): SecretKey = {
    val keyGenerator = KeyGenerator.getInstance(algorithm, "BC")
    keyGenerator.init(keyBitLength)
    keyGenerator.generateKey()
  }

  def saveMasterSecret(passphrase: String, masterSecret: MasterSecret, filePath: String): Unit = {
    val salt = generateSalt()
    val masterSecretBytes = encodeMasterSecret(masterSecret)
    val encMasterSecretBytes = computeFromPassphrase(Cipher.ENCRYPT_MODE, passphrase, salt, masterSecretBytes)
    val oos = new ObjectOutputStream(new FileOutputStream(filePath))
    oos.writeObject(salt)
    oos.writeObject(encMasterSecretBytes)
  }

  def loadMasterSecret(passphrase: String, filePath: String): MasterSecret = {
    val ois = new ObjectInputStream(new FileInputStream(filePath))
    val salt = ois.readObject().asInstanceOf[Array[Byte]]
    val encMasterSecretEncoded = ois.readObject().asInstanceOf[Array[Byte]]
    val masterSecretEncoded = computeFromPassphrase(Cipher.DECRYPT_MODE, passphrase, salt, encMasterSecretEncoded)
    decodeMasterSecret(masterSecretEncoded)
  }

  private def computeFromPassphrase(cipherMode: Int, passphrase: String, salt: Array[Byte], data: Array[Byte]): Array[Byte] = {
    val aesKey = deriveAesKeyFromPassphrase(passphrase, salt)
    val cipher = Cipher.getInstance(aesKey.getAlgorithm)
    cipher.init(cipherMode, aesKey, new PBEParameterSpec(salt, passphraseIterations))
    cipher.doFinal(data)
  }

  private def deriveAesKeyFromPassphrase(passphrase: String, salt: Array[Byte]): SecretKey = {
    val keyFactory = SecretKeyFactory.getInstance("PBKDF2WITHHMACSHA256","BC")
    val keySpec = new PBEKeySpec(passphrase.toCharArray, salt, passphraseIterations, hmacKeyBitLength)
    val keyBytes = keyFactory.generateSecret(keySpec).getEncoded
    new SecretKeySpec(keyBytes, aesAlgorithm)
  }

  private def generateSalt(): Array[Byte] = {
    val saltByteSize = hmacKeyBitLength / 8
    val random = SecureRandom.getInstance("DEFAULT", "BC")
    val salt = Array.ofDim[Byte](saltByteSize)
    random.nextBytes(salt)
    salt
  }
}
