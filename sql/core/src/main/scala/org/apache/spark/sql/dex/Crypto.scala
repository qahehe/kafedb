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
import java.security.SecureRandom

import javax.crypto.{Cipher, KeyGenerator, SecretKey, SecretKeyFactory}
import javax.crypto.spec.{PBEKeySpec, PBEParameterSpec, SecretKeySpec}
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

  val aesBlockByteSize: Int = 16
  val ivBytesSize: Int = aesBlockByteSize
  val saltByteSize: Int = 16
  val iterationCount: Int = 1 << 17 // 131,072
  val passphraseIterations: Int = 1 << 7 // 128
  val aesKeyBitLength = 128

  case class MasterSecret(key: SecretKey) {
    def data: Seq[Byte] = key.getEncoded
  }

  def generateMasterSecret(): MasterSecret = {
    MasterSecret(generateEncryptionSecret())
  }

  private def generateEncryptionSecret(): SecretKey = {
    val keyGenerator = KeyGenerator.getInstance("AES", "BC")
    keyGenerator.init(aesKeyBitLength)
    keyGenerator.generateKey()
  }

  def save(passphrase: String, masterSecret: MasterSecret, filePath: String): Unit = {
    val salt = generateSalt()
    val encMasterKeyBytes = encryptFromPassphrase(passphrase, salt, masterSecret.data)
    val oos = new ObjectOutputStream(new FileOutputStream(filePath))
    oos.writeObject(salt)
    oos.writeObject(encMasterKeyBytes)
  }

  def load(passphrase: String, filePath: String): MasterSecret = {
    val ois = new ObjectInputStream(new FileInputStream(filePath))
    val salt = ois.readObject().asInstanceOf[Seq[Byte]]
    val encMasterKeyBytes = ois.readObject().asInstanceOf[Seq[Byte]]
    val masterKeyBytes = decryptFromPassphrase(passphrase, salt, encMasterKeyBytes)
    val masterKey = new SecretKeySpec(masterKeyBytes.toArray, "AES")
    MasterSecret(masterKey)
  }

  private def encryptFromPassphrase(passphrase: String, salt: Seq[Byte], data: Seq[Byte]): Seq[Byte] = {
    val key = deriveKeyFromPassphrase(passphrase, salt: Seq[Byte])
    val cipher = Cipher.getInstance(key.getAlgorithm)
    cipher.init(Cipher.ENCRYPT_MODE, key, new PBEParameterSpec(salt.toArray, iterationCount))
    cipher.doFinal(data.toArray)
  }

  private def decryptFromPassphrase(passphrase: String, salt: Seq[Byte], data: Seq[Byte]): Seq[Byte] = {
    val key = deriveKeyFromPassphrase(passphrase, salt: Seq[Byte])
    val cipher = Cipher.getInstance(key.getAlgorithm)
    cipher.init(Cipher.DECRYPT_MODE, key, new PBEParameterSpec(salt.toArray, iterationCount))
    cipher.doFinal(data.toArray)
  }

  private def deriveKeyFromPassphrase(passphrase: String, salt: Seq[Byte]): SecretKey = {
    val keyFactory = SecretKeyFactory.getInstance("PBEWITHSHA1AND128BITAES-CBC-BC")
    val keySpec = new PBEKeySpec(passphrase.toCharArray, salt.toArray, iterationCount)
    keyFactory.generateSecret(keySpec) //.getEncoded()
  }

  private def generateSalt(): Seq[Byte] = {
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