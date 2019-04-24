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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, BinaryComparison, BinaryExpression, BindReferences, BoundReference, EqualTo, ExpectsInputTypes, ExprId, Expression, IsNotNull, JoinedRow, Literal, NamedExpression, Or, Predicate}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation}
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

  private val tM = LogicalRelation(
    DataSource.apply(
      sparkSession,
      className = "jdbc",
      options = Map(
        JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
        JDBCOptions.JDBC_TABLE_NAME -> "tm")).resolveRelation())

  private val decKey = Literal.create("dummy_dec_key", StringType)
  private val attrPrfKey = Literal.create("dummy_attr_prf_key", StringType)

  private val resolver = sparkSession.sqlContext.conf.resolver

  //val decryptValueToRid = functions.udf((value: String) => """(.+)_enc$""".r.findFirstIn(value).get)
  //sparkSession.udf.register("decryptValueToRid", decryptValueToRid)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = Seq(
    // todo first need to move/coallese the DexPlan operators
    Batch("Unresolve Non-Dex Part of Query", Once, UnresolveDexPlanAncestors),
    Batch("Translate Dex Query", Once, TranslateDexQuery)
  )

  object UnresolveDexPlanAncestors extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val dexOutputSet = plan.collectFirst { case p: DexPlan => p.outputSet } get
      var foundDexPlan = false
      plan transformDown {
        case p: DexPlan =>
          foundDexPlan = true
          p
        case p: LogicalPlan if !foundDexPlan =>
          p.transformExpressions {
            case a: Attribute if dexOutputSet.contains(a) => UnresolvedAttribute(a.name)
            case expr => expr
          }
      }
    }
  }

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

    def translate: LogicalPlan = analyze {
      val newPlan = translatePlan(dexPlan.child, None)
      newPlan.select(output: _*)
    }

    private def translateAttribute(attr: Attribute): NamedExpression = {
      Decrypt(decKey, attrEncOf(attr)).cast(attr.dataType).as(attr.name)
    }

    private def attrEncOf(attr: Attribute): Attribute = $"${attr.name}_prf"

    private def analyze(plan: LogicalPlan) =
      sparkSession.sessionState.analyzer.executeAndCheck(plan)

    private def translatePlan(plan: LogicalPlan, childView: Option[LogicalPlan]): LogicalPlan = analyze {
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
          translatePlan(f.child, translateFormula(FilterFormula, f.condition, childView))

        case j: Join if j.joinType == Cross =>
          translatePlan(j.left, childView).join(translatePlan(j.right, childView), Cross)

        case j: Join if j.condition.isDefined =>
          val joinAttrs = nonIsNotNullPredsIn(Seq(j.condition.get))
          val leftAttrs = nonIsNotNullPredsIn(j.left.expressions)
          val rightAttrs = nonIsNotNullPredsIn(j.right.expressions)

          if (joinAttrs.equals(leftAttrs ++ rightAttrs)) {
            // join completely coincides with filters
            // e.g. T1(a, b) join T2(c, d) on a = c and b = d where a = c = 1 and b = d = 2
            // note: this cross join only works for equality filter and joins
            val leftView = translatePlan(j.left, childView)
            val rightView = translatePlan(j.right, childView)
            leftView.join(rightView, Cross)
          } else {
            // e.g.  T1(a, b) join T2(c, d) on a = c and b = d where a = c = 1
            val leftView = translatePlan(j.left, childView)
            val joinView = translateFormula(JoinFormula, j.condition.get, Some(leftView))
            translatePlan(j.right, joinView)
          }

        case _: DexPlan => throw DexException("shouldn't get DexPlan in subtree of a DexPlan")

        case x => throw DexException("unsupported: " + x.toString)
      }
    }

    private def nonIsNotNullPredsIn(conds: Seq[Expression]): AttributeSet = {
      conds.flatMap(_.collect {
        case x: BinaryComparison => x.references
      }).reduceOption(_ ++ _).getOrElse(AttributeSet.empty)
    }

    private def translateFormula(formulaType: FormulaType, condition: Expression, childView: Option[LogicalPlan]): Option[LogicalPlan] = {
      condition match {
        case p: EqualTo => formulaType match {
          case FilterFormula =>
            Some(translateFilterPredicate(p, childView))
          case JoinFormula =>
            Some(translateJoinPredicate(p, childView))
        }

        case And(left, right) =>
          translateFormula(formulaType, right, translateFormula(formulaType, left, childView))

        case Or(left, right) =>
          val lt = translateFormula(formulaType, left, childView)
          val rt = translateFormula(formulaType, right, childView)
          rt.flatMap(r => lt.map(l => l unionDistinct r)).orElse(lt)

        case IsNotNull(attr: Attribute) =>
          childView

        case x => throw DexException("unsupported: " + x.toString)
      }
    }.map(analyze)

    private def translateFilterPredicate(p: Predicate, childView: Option[LogicalPlan]): LogicalPlan = p match {
      case EqualTo(left: Attribute, right@Literal(value, dataType)) =>
        val colName = left.name
        val tableName = exprIdToTable(left.exprId)
        val ridOrder = s"rid_${joinOrder(tableName)}"
        val valueStr = dataType match {
          case IntegerType => value.asInstanceOf[Int]
          case StringType => value
          case x => throw DexException("unsupported: " + x.toString)
        }

        /*val predicateRelation = LocalRelation(
          LocalRelation('predicate.string).output,
          InternalRow(UTF8String.fromString(s"$tableName~$colName~$valueStr")) :: Nil)*/
        val predicate = s"$tableName~$colName~$valueStr"
        cashTSelectOf(predicate, ridOrder, childView)

      case EqualTo(left: Attribute, right: Attribute) if left.name < right.name =>
        val colNames = (left.name, right.name)
        val tableNames = (exprIdToTable(left.exprId), exprIdToTable(right.exprId))
        require(tableNames._1 == tableNames._2)
        val tableName = tableNames._1
        val ridOrder = s"rid_${joinOrder(tableName)}"
        val predicate = s"$tableName~${colNames._1}~${colNames._2}"
        cashTSelectOf(predicate, ridOrder, childView)

      case EqualTo(left: Attribute, right: Attribute) if left.name > right.name =>
        translateFilterPredicate(EqualTo(right, left), childView)

      case x => throw DexException("unsupported: " + x.toString)
    }

    private def cashTSelectOf(predicate: String, ridOrder: String, childView: Option[LogicalPlan]): LogicalPlan = {
      val cashTSelect = CashTSelect(predicate, tSelect)
        .select(Decrypt(decKey, $"value").as(ridOrder))

      childView match {
        case None =>
          cashTSelect
        case Some(cw) =>
          require(cw.output.exists(_.name == ridOrder))
          cw.join(cashTSelect, UsingJoin(LeftSemi, Seq(ridOrder)))
      }
    }

    private def translateJoinPredicate(p: Predicate, childView: Option[LogicalPlan]): LogicalPlan = p match {
      case EqualTo(left: Attribute, right: Attribute) =>
        childView match {
          case Some(cw) =>
            val (leftColName, rightColName) = (left.name, right.name)
            val (leftTableName, rightTableName) = (exprIdToTable(left.exprId), exprIdToTable(right.exprId))
            val (leftRidOrder, rightRidOrder) = (s"rid_${joinOrder(leftTableName)}", s"rid_${joinOrder(rightTableName)}")
            val (leftRidOrderAttr, rightRidOrderAttr) = (
              cw.output.find(_.name == leftRidOrder),
              cw.output.find(_.name == rightRidOrder))

            val predicate = s"$leftTableName~$leftColName~$rightTableName~$rightColName"

            (leftRidOrderAttr, rightRidOrderAttr) match {
              case (Some(l), None) =>
                val cashTm = CashTM(predicate, cw, tM, l)
                val cashTmProject = cashTm.output.collect {
                  case x: Attribute if x.name == "value" => Decrypt($"rid", x).as(rightRidOrder)
                  case x: Attribute if x.name != "rid" => x
                }
                cashTm.select(cashTmProject: _*)

              case (Some(l), Some(r)) =>
                val cashTm = CashTM(predicate, cw, tM, l).where(EqualTo(r, Decrypt($"rid", $"value")))
                val cashTmProject = cw.output
                cashTm.select(cashTmProject: _*)

              case _ => ???
            }
          case None =>
            // join has to have childView, at least from the data source scan
            throw DexException("Shouldn't be here")
        }
    }

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

  override def output: Seq[Attribute] = emm.output

  /*override def requiredChildDistribution: Seq[Distribution] = emmType match {
    case EmmTSelect =>
       UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
    case _ => ???
  }*/

  override def child: SparkPlan = emm
}

case class CashTMExec(predicate: String, childView: SparkPlan, emm: SparkPlan, childViewRid: Attribute) extends BinaryExecNode {
  override def left: SparkPlan = childView

  override def right: SparkPlan = emm

  /**
    * Produces the result of the query as an `RDD[InternalRow]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalRow] = {
    val childViewRidCol = BindReferences.bindReference(childViewRid, childView.output).asInstanceOf[BoundReference]
    val emmRidCol = BindReferences.bindReference(emm.output.head, emm.output).asInstanceOf[BoundReference]
    val emmValueCol = BindReferences.bindReference(emm.output.apply(1), emm.output).asInstanceOf[BoundReference]

    // need to materialize internal rows via copy() for jdbc rdd
    // todo: differentiate first join vs conjunctive joins, latter case need not copy
    // todo: cache?
    val childViewRdd = childView.execute().map(_.copy())
    val emmRdd =
      emm.execute().map(row => (emmRidCol.eval(row).asInstanceOf[UTF8String], row.copy()))

    var childViewRddToCount = childViewRdd
    Iterator.from(0).map { i =>
      // iteratively "shrink'" the childViewRdd by the result of each join
      val res = childViewRddToCount.map { row =>
        val rid = childViewRidCol.eval(row).asInstanceOf[UTF8String].toString
        val ridPredicate = s"$predicate~$rid"
        (UTF8String.fromString(s"$ridPredicate~$i"), (UTF8String.fromString(ridPredicate), row))
      }.join(emmRdd).values.map { case ((ridPredicate, childViewRow), emmRow) =>
        val emmValue = emmValueCol.eval(emmRow)
        val joinedValues = childViewRow.toSeq(childView.schema) ++ Seq(ridPredicate, emmValue)
        (childViewRow, InternalRow.fromSeq(joinedValues))
      }
      childViewRddToCount = res.keys
      res.values
    }.takeWhile(!_.isEmpty()).reduceOption(_ ++ _).getOrElse(sparkContext.emptyRDD)
  }

  override def output: Seq[Attribute] = left.output ++ right.output
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

sealed trait FormulaType
case object JoinFormula extends FormulaType
case object FilterFormula extends FormulaType


case class DexException(msg: String) extends RuntimeException(msg)
