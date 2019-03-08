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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.IntegerType

/* case class EncryptDatabase(databaseName: String, path: Option[String])
  extends RunnableCommand with DexCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    require(catalog.databaseExists(databaseName))

    val tableIds = catalog.listTables(databaseName)

    tableIds.map { tableId =>
      encryptTable(sparkSession, tableId)
    }
    Seq.empty
  }
} */

case class EncryptTableCommand(tableName: String, child: LogicalPlan) extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import sparkSession.implicits._


    // execute the only dependency
    val qe = sparkSession.sessionState.executePlan(child)
    qe.assertAnalyzed()

    // val schema = catalog.getTableMetadata(table).schema
    // val schemaEnc = new StructType()
    // sparkSession.table(table).
    val table = sparkSession.table(tableName)
    // val encoder = RowEncoder(table.schema)

    val tableWithRid = table.withColumn("rid", monotonically_increasing_id())

    table.schema.foreach { attr =>
      val colRid = tableWithRid.select(attr.name, "rid")
      val tSelect = attr.dataType match {
        case IntegerType =>
          colRid.as[(Int, BigInt)].groupByKey(_._1).flatMapGroups {
            case (value, rids) =>
              rids.zipWithIndex.map {
                case ((_, rid), counter) => (s"$value~$counter", rid)
              }
          }
      }
      // todo: store encrypted tables to a designated database.
      // for now: store as global temporary views
      tSelect.toDF("rid", "value").persist().createTempView(s"tselect_${tableName}_${attr.name}")
    }

    val encColNames = tableWithRid.schema.map(x => if (x.name != "rid") s"${x.name}_enc" else "rid")
    val tableEnc = tableWithRid.toDF(encColNames: _*)
    tableEnc.persist().createTempView(s"${tableName}_enc")

    Seq.empty
  }
}
