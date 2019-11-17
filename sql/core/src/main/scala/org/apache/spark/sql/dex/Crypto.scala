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

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.security.{MessageDigest, SecureRandom}

import javax.crypto.{Cipher, KeyGenerator, Mac, SecretKey, SecretKeyFactory}
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, PBEParameterSpec, SecretKeySpec}
import org.apache.spark.sql.catalyst.expressions.{Concat, DialectSQLTranslatable, Expression, Literal}
import org.apache.spark.sql.dex.DexConstants.TableAttribute

object Crypto {
  // dynamic installation of bouncy castle provider
  java.security.Security.addProvider(
    new org.bouncycastle.jce.provider.BouncyCastleProvider())

  def prf(k: String)(m: Any): String = {
    s"${m}_$k"
  }

  def symEnc(k: String)(m: Any): String = {
    s"${m}_$k"
  }

/*  def prf(masterSecret: MasterSecret)(m: Any): Array[Byte] = {
    //s"${m}_$k"
    masterSecret.computeHmac(DataCodec.toByteArray(m))
  }

  def symEnc(masterSecret: MasterSecret)(m: Any): Array[Byte] = {
    //s"${m}_$k"
    masterSecret.encryptAesCbc(DataCodec.toByteArray(m))
  }*/

  val passphraseIterations: Int = 1 << 10 // 1024
  val aesKeyBitLength = 128
  val aesAlgorithm = "AES"
  val hmacKeyBitLength = 256
  val hmacAlgorithm = "HmacSHA256"

  @SerialVersionUID(1L)
  case class MasterSecret(aesKey: SecretKey, hmacKey: SecretKey) extends Serializable {
    require(aesKey.getEncoded.length == aesKeyBitLength / 8)
    require(aesKey.getAlgorithm.toLowerCase == aesAlgorithm.toLowerCase)
    require(hmacKey.getEncoded.length == hmacKeyBitLength / 8)
    require(hmacKey.getAlgorithm.toLowerCase == hmacAlgorithm.toLowerCase)

    def computeHmac(data: Array[Byte]): Array[Byte] = {
      val mac = Mac.getInstance(hmacAlgorithm, "BC")
      mac.init(hmacKey)
      mac.update(data)
      mac.doFinal()
    }

    def encryptAesCbc(data: Array[Byte]): Array[Byte] = {
      val cipher = Cipher.getInstance("AES/CBC/PKCS7Padding", "BC")
      cipher.init(Cipher.ENCRYPT_MODE, aesKey)
      val iv = cipher.getIV
      val output = cipher.doFinal(data)
      DataCodec.concatBytes(iv, output)
    }

    def decryptAesCbc(ciphertext: Array[Byte]): Array[Byte] = {
      val cipher = Cipher.getInstance("AES/CBC/PKCS7Padding", "BC")
      val iv = ciphertext.take(cipher.getBlockSize)
      cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(iv))
      cipher.doFinal(ciphertext)
    }
  }

  // Do not objectize this method to singleton of MasterSecret,
  // because if so and if it is called before the Crypto, the bouncystle provider is not yet added
  // the Crypto singleton object constructor.
  def generateMasterSecret(): MasterSecret = {
    val aesKey = generateSecret(aesAlgorithm, aesKeyBitLength)
    val hmacKey = generateSecret(hmacAlgorithm, hmacKeyBitLength)
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

object DexPrimitives {

  val prfKey = "prf"
  val symEncKey = "enc"

  def dexTableNameOf(tableName: String): String = Crypto.prf(prfKey)(tableName)

  def dexColNameOf(colName: String): String = Crypto.prf(prfKey)(colName)

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

  def dexEmmLabelOf(dexPredicate: String, counter: Int): String = {
    // todo: use dexEmmLabelPrfKey(dexPredicate) for k, return F_k(counter)
    s"${dexEmmLabelPrfKeyOf(dexPredicate)}~$counter"
  }

  def dexEmmValueOf(dexPredicate: String, value: Number): String = {
    Crypto.symEnc(dexEmmValueEncKeyOf(dexPredicate))(s"$value")
  }

  def dexCellOf(cell: String): String = {
    Crypto.symEnc(symEncKey)(cell)
  }

  def dexRidOf(rid: Number): String = {
    Crypto.prf(prfKey)(rid)
  }

  def dexEmmLabelPrfKeyOf(dexPredicate: String): String = dexPredicate // todo: append 1 and apply F
  def dexEmmValueEncKeyOf(dexpredicate: String): String = "enc" // todo: append 2 and apply F

  def sqlEmmLabelPrfKeyExprOf(predicateExpr: DialectSQLTranslatable): DialectSQLTranslatable  = {
    // todo: use Postgres PRF on key=client secret key append 1
    predicateExpr
  }

  def sqlEmmValueEncKeyExprOf(predicateExpr: DialectSQLTranslatable): DialectSQLTranslatable = {
    // todo: use Postgres PRF on key=client secret key append 2
    //"'enc'"
    Literal("enc")
  }

  def sqlEmmLabelExprOf(dbEmmLabelPrfKeyExpr: DialectSQLTranslatable, counterExpr: DialectSQLTranslatable): DialectSQLTranslatable  = {
    // todo: instead of concat, use Postgres PRF on key=dbEmmLabelprfKeyExpr
    //s"$dbEmmLabelPrfKeyExpr || '~' || $counterExpr"
    Concat(dbEmmLabelPrfKeyExpr :: Literal("~") :: counterExpr :: Nil)
  }

  def sqlConcatPredicateExprsOf(predicatePrefixExpr: DialectSQLTranslatable, predicateExpr: DialectSQLTranslatable): DialectSQLTranslatable = {
    //s"$predicatePrefixExpr || '~' || $predicateExpr"
    Concat(predicatePrefixExpr :: Literal("~") :: predicateExpr :: Nil)
  }

  def sqlDecryptExpr(encKeyExpr: String, colExpr: String): String =
  // todo: use SQL decrypt like s"decrypt($decKey, $col)"
    s"substring($colExpr, '(.+)_' || $encKeyExpr)"
}