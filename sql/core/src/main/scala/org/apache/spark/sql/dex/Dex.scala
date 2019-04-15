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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryExpression, BindReferences, BoundReference, EqualTo, ExpectsInputTypes, ExprId, Expression, IsNotNull, JoinedRow, Literal, NamedExpression, Or, Predicate}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

class Dex(sessionCatalog: SessionCatalog, sparkSession: SparkSession) extends RuleExecutor[LogicalPlan] {

  private val tSelect = LogicalRelation(
    DataSource.apply(
      sparkSession,
      className = "jdbc",
      options = Map(
        JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
        JDBCOptions.JDBC_TABLE_NAME -> "tselect")).resolveRelation())

  private val tSelectDf = Dataset.ofRows(sparkSession, tSelect)

  private val decKey = Literal.create("dummy_dec_key", StringType)
  private val attrPrfKey = Literal.create("dummy_attr_prf_key", StringType)


  //val decryptValueToRid = functions.udf((value: String) => """(.+)_enc$""".r.findFirstIn(value).get)
  //sparkSession.udf.register("decryptValueToRid", decryptValueToRid)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = Seq(
    // todo first need to move/coallese the DexPlan operators
    Batch("Translate Dex Query", Once, TranslateDexQuery)
  )

  object TranslateDexQuery extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan transformDown {
        case p: DexPlan =>
          val translator = new DexPlanTranslator(p)
          translator.translate
      }
    }
  }

  class DexPlanTranslator(dexPlan: DexPlan) {

    private lazy val joinOrder: String => Int =
      dexPlan.collect {
        case LogicalRelation(relation: JDBCRelation, _, _, _) =>
          relation.jdbcOptions.tableOrQuery
      }.indexOf

    private lazy val exprIdToTable: ExprId => String =
      dexPlan.collect {
        case l @ LogicalRelation(relation: JDBCRelation, _, _, _) =>
          l.output.map(x => (x.exprId, relation.jdbcOptions.tableOrQuery))
      }.flatten.toMap

    private lazy val output = dexPlan.output.map(translateAttribute)

    def translate: LogicalPlan = {
      translatePlan(dexPlan.child, None).select(output: _*)
    }

    private def translateAttribute(attr: Attribute): NamedExpression = {
      Decrypt(decKey, attrEncOf(attr)).cast(attr.dataType).as(attr.name)
    }

    private def attrEncOf(attr: Attribute): Attribute = $"${attr.name}_prf"

    private def translatePlan(plan: LogicalPlan, childView: Option[LogicalPlan]): LogicalPlan = {
      plan match {
        case l: LogicalRelation =>
          l.relation match {
            case j: JDBCRelation =>
              val tableName = j.jdbcOptions.tableOrQuery
              val tableEnc = tableEncOf(tableName)
              childView match {
                case Some(w) =>
                  w.join(tableEnc, NaturalJoin(LeftOuter))
                case None =>
                  tableEnc
              }
            case x => throw DexException("unsupported: " + x.toString)
          }

        case p: Project =>
          // todo: projection push down
          translatePlan(p.child, childView)

        case f: Filter =>
          translatePlan(f.child, translateFormula(f.condition, childView))

        case j: Join if j.joinType == Cross =>
          translatePlan(j.left, childView).join(translatePlan(j.right, childView), Cross)

        case j: Join if j.condition.isDefined =>
          val leftView = translatePlan(j.left, childView)
          val joinView = translateFormula(j.condition.get, Some(leftView))
          translatePlan(j.right, joinView)

        case _: DexPlan => throw DexException("shouldn't get DexPlan in subtree of a DexPlan")

        case x => throw DexException("unsupported: " + x.toString)
      }
    }

    private def translateFormula(condition: Expression, childView: Option[LogicalPlan]): Option[LogicalPlan] = {
      condition match {
        case p @ EqualTo(_: Attribute, _: Literal) =>
          Some(translateFilterPredicate(p, childView))

        case p @ EqualTo(_: Attribute, _: Attribute) =>
          Some(translateJoinPredicate(p, childView))

        case And(left, right) =>
          translateFormula(right, translateFormula(left, childView))

        case Or(left, right) =>
          val lt = translateFormula(left, childView)
          val rt = translateFormula(right, childView)
          rt.flatMap(r => lt.map(l => l unionDistinct r)).orElse(lt)

        case IsNotNull(attr: Attribute) =>
          None

        case x => throw DexException("unsupported: " + x.toString)
      }
    }

    private def translateFilterPredicate(p: Predicate, childView: Option[LogicalPlan]): LogicalPlan = p match {
      case EqualTo(left: AttributeReference, right @ Literal(value, dataType)) =>
        childView match {
          case None =>
            val colName = left.name
            val tableName = exprIdToTable(left.exprId)
            val ridOrder = s"rid_${joinOrder(tableName)}"
            // todo: use Cash et al counter
            val valueStr = dataType match {
              case IntegerType => value.asInstanceOf[Int]
              case StringType => value
              case x => throw DexException("unsupported: " + x.toString)
            }

            /*val predicateRelation = LocalRelation(
              LocalRelation('predicate.string).output,
              InternalRow(UTF8String.fromString(s"$tableName~$colName~$valueStr")) :: Nil)*/
            val predicate = s"$tableName~$colName~$valueStr"

            CashTSelect(predicate, tSelect)
              .select(Decrypt(decKey, $"value").as(ridOrder))

            //predicateRelation.generate(CashCounterForTSelect("predicate", tSelectDf), outputNames = Seq("value"))
            //  .select(Decrypt(decKey, $"value").as(ridOrder))

            /*tSelect
              .select(Decrypt(decKey, $"value").as(ridOrder))
              .where(EqualTo($"rid", s"$tableName~$colName~$valueStr~counter"))*/
          case Some(w) =>
            throw DexException("todo: " + w.toString)
        }

      case x => throw DexException("unsupported: " + x.toString)
    }

    private def translateJoinPredicate(p: Predicate, childView: Option[LogicalPlan]): LogicalPlan = ???

    private def tableEncOf(tableName: String): LogicalPlan = {
      val tableEncName = s"${tableName}_prf"
      val tableEnc = LogicalRelation(
        DataSource.apply(
          sparkSession,
          className = "jdbc",
          options = Map(
            JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
            JDBCOptions.JDBC_TABLE_NAME -> tableEncName)).resolveRelation())
      renameRidWithJoinOrder(tableEnc, tableName)
    }

    private def renameRidWithJoinOrder(tableEnc: LogicalRelation, tableName: String): LogicalPlan = {
      val resolver = sparkSession.sessionState.analyzer.resolver
      val output = tableEnc.output
      val columns = output.map { col =>
        if (resolver(col.name, "rid")) {
          Column(col).as(s"rid_${joinOrder(tableName)}").expr
        } else {
          Column(col).expr
        }
      }
      tableEnc.select(columns: _*)
    }

  }
}

case class Decrypt(key: Expression, value: Expression) extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  override def left: Expression = key
  override def right: Expression = value
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    //val fromCharset = input2.asInstanceOf[UTF8String].toString
    //UTF8String.fromString(new String(input1.asInstanceOf[Array[Byte]], fromCharset))
     UTF8String.fromString("""(.+)_enc$""".r.findFirstMatchIn(input2.asInstanceOf[UTF8String].toString).get.group(1))
  }

  /*override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (key, value) =>
      s"""
          ${ev.value} = $value.split(UTF8String.fromString("_enc"), 0)[0];
      """)
  }*/
}

case class CashTSelectExec(predicate: String, emm: SparkPlan) extends UnaryExecNode {

  private val cashCondition: Int => InternalRow => Boolean = {
    cashCounter => emmRow => {
      val lhs = UTF8String.fromString(s"$predicate~$cashCounter")
      val emmRidCol = BindReferences.bindReference(emm.output.head, emm.output).asInstanceOf[BoundReference]
      val ordering = TypeUtils.getInterpretedOrdering(emmRidCol.dataType)
      val rhs = emmRidCol.eval(emmRow)
      ordering.equiv(lhs, rhs)
    }
  }

  /**
    * Produces the result of the query as an `RDD[InternalRow]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalRow] = {
    val emmRdd = emm.execute()
    Iterator.from(0).map { i =>
      emmRdd.mapPartitionsInternal { emmIter =>
        emmIter.find(cashCondition(i)).iterator
      }
    }.takeWhile(!_.isEmpty()).reduceOption(_ ++ _).getOrElse(sparkContext.emptyRDD)
  }

  // todo: for t_m of joinning a new table, need to add new rid to output
  override def output: Seq[Attribute] = emm.output

  /*override def requiredChildDistribution: Seq[Distribution] = emmType match {
    case EmmTSelect =>
       UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
    case _ => ???
  }*/

  override def child: SparkPlan = emm
}


// case class TSelect(rid: String, value: String)

/*case class CashCounterForTSelect(child: Expression, tSelect: DataFrame) extends UnaryExpression with CollectionGenerator with CodegenFallback with Serializable {
  /** The position of an element within the collection should also be returned. */
  override val position: Boolean = false

  /** Rows will be inlined during generation. */
  override val inline: Boolean = false

  /**
    * The output element schema.
    */
  override def elementSchema: StructType = child.dataType match {
    case _: StringType => new StructType().add("value", StringType, nullable = false)
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val predicate = child.eval(input).asInstanceOf[UTF8String].toString

    new Iterator[InternalRow] {
      private var counter = 0
      private def nextQuery() = tSelect.select("value").where(s"rid = $predicate~$counter").limit(1)

      override def hasNext: Boolean = !nextQuery().isEmpty

      override def next(): InternalRow = {
        val res = nextQuery().head()
        counter += 1
        InternalRow(res.toSeq)
      }
    }
  }
}*/


case class DexException(msg: String) extends RuntimeException(msg)
