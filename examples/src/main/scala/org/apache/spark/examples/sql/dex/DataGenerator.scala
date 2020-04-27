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