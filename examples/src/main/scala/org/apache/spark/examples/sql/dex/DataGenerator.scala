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
package org.apache.spark.examples.sql.dex
// scalastyle:off

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait DataGenerator extends Serializable {
  def generate(
                sparkContext: SparkContext,
                name: String,
                partitions: Int,
                scaleFactor: String): RDD[String]
}

class DBGEN(dbgenDir: String, params: Seq[String]) extends DataGenerator {
  val dbgen = s"$dbgenDir/dbgen"
  def generate(sparkContext: SparkContext,name: String, partitions: Int, scaleFactor: String) = {
    val smallTables = Seq("nation", "region")
    val numPartitions = if (partitions > 1 && !smallTables.contains(name)) partitions else 1
    val generatedData = {
      sparkContext.parallelize(1 to numPartitions, numPartitions).flatMap { i =>
        val localToolsDir = if (new java.io.File(dbgen).exists) {
          dbgenDir
        } else if (new java.io.File(s"/$dbgenDir").exists) {
          s"/$dbgenDir"
        } else {
          sys.error(s"Could not find dbgen at $dbgen or /$dbgenDir. Run install")
        }
        val parallel = if (numPartitions > 1) s"-C $partitions -S $i" else ""
        val shortTableNames = Map(
          "customer" -> "c",
          "lineitem" -> "L",
          "nation" -> "n",
          "orders" -> "O",
          "part" -> "P",
          "region" -> "r",
          "supplier" -> "s",
          "partsupp" -> "S"
        )
        val paramsString = params.mkString(" ")
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dbgen -q $paramsString -T ${shortTableNames(name)} -s $scaleFactor $parallel")
        println(commands)
        BlockingLineStream(commands)
      }.repartition(numPartitions)
    }

    generatedData.setName(s"$name, sf=$scaleFactor, strings")
    generatedData
  }
}