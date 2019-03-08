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

import org.scalactic.source
import org.scalatest.Tag

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

trait EncryptQueryTest extends QueryTest with SharedSQLContext {

  protected override def sparkConf = super.sparkConf
    .set(SQLConf.CODEGEN_FACTORY_MODE, CodegenObjectFactoryMode.NO_CODEGEN.toString)
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)

  // Whether to materialize all encrypted test data before the first test is run
  private var encryptTestDataBeforeTests = false

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (encryptTestDataBeforeTests) {
      testData2.encrypt("testData2")
    }
  }

  protected override def setupTestData(): Unit = {
    super.setupTestData()
    encryptTestDataBeforeTests = true
  }

  /* override protected def test(
       testName: String,
       testTags: Tag*)(testFun: => Any)(implicit pos: source.Position): Unit = {
    val interpretedMode = CodegenObjectFactoryMode.NO_CODEGEN.toString
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> interpretedMode) {
      super.test(testName + " (interpreted path)", testTags: _*)(testFun)(pos)
    }
  } */
}
