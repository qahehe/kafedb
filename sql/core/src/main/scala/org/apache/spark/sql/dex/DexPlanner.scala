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

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, BinaryComparison, BinaryExpression, BindReferences, BoundReference, EqualTo, ExpectsInputTypes, ExprId, Expression, In, IsNotNull, JoinedRow, Literal, NamedExpression, Not, Or, Predicate, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

class DexPlanner(sessionCatalog: SessionCatalog, sparkSession: SparkSession) extends RuleExecutor[LogicalPlan] {

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

  private val emmTables = Set(tSelect, tM)

  //private val decKey = Literal.create("dummy_dec_key", StringType)
  private val metadataDecKey = "metadata_dec_key"
  private val emmDecKeyPrefix = "emm_dec_key_prefix"
  private val attrPrfKey = Literal.create("dummy_attr_prf_key", StringType)

  private val resolver = sparkSession.sqlContext.conf.resolver

  //val decryptValueToRid = functions.udf((value: String) => """(.+)_enc$""".r.findFirstIn(value).get)
  //sparkSession.udf.register("decryptValueToRid", decryptValueToRid)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = Seq(
    // todo first need to move/coallese the DexPlan operators
    Batch("Unresolve Non-Dex Part of Query", Once, UnresolveDexPlanAncestors),
    Batch("Translate Dex Query", Once, TranslateDexQuery),
    Batch("Delay Data Table", Once, DelayDataTableLeftSemiJoinAfterFilters),
    Batch("Remove DexPlan node", Once, RemoveDexPlanNode)
  )

  /*
  == Dex Plan ==
Project [cast(decrypt(metadata_dec_key, b_prf#13) as int) AS b#16]
+- Project [rid_0#14, a_prf#12, b_prf#13]
   +- Join LeftSemi, (rid_0#14 = rid_0#15)
      :- Project [rid#11 AS rid_0#14, a_prf#12, b_prf#13]
      :  +- Relation[rid#11,a_prf#12,b_prf#13] JDBCRelation(testdata2_prf) [numPartitions=1]
      +- Project [decrypt(emm_dec_key_prefix~testdata2~a~2, value#8) AS rid_0#15]
         +- CashTSelect testdata2~a~2
            +- Relation[rid#7,value#8] JDBCRelation(tselect) [numPartitions=1]

   SELECT decrypt(metadata_dec_key, b_prf) as int) as b
   FROM (
   // DEX plan starts from here
     SELECT rid_0, a_prf, b_prf
     FROM (
       SELECT rid as rid_0, a_prf, b_prf
       FROM testdata2_prf
     ) AS ???
     JOIN (
       SELECT udf_derypt(emm_dec_key_prefix~testdata2~a~s, value) AS rid_0
       FROM udf_select(testdata2~a~2)
     ) AS ???
   )
   */
  object ConvertDexPlanToSQL extends Rule[LogicalPlan] {

    private val dialect = JdbcDialects.get(JDBCOptions.JDBC_URL)

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transformDown {
        case p: DexPlan =>
          val sql = convertToSQL(p)
          val jdbcOption = new JDBCOptions(Map(
            JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
            JDBCOptions.JDBC_QUERY_STRING -> sql))
          val baseRelation = JDBCRelation(p.schema, Array.empty, jdbcOption)(sparkSession)
          LogicalRelation(baseRelation)
      }
    }

    private def convertToSQL(plan: LogicalPlan): String = plan match {
      case p: DexPlan => convertToSQL(p.child)
      case p: Project =>
        val projectList = p.projectList.map(_.sql)
        s"SELECT ${projectList} FROM " ++ convertToSQL(p.child)
      case p: LogicalRelation =>
        p.relation match {
          case j: JDBCRelation => j.jdbcOptions.tableOrQuery
          case _ => throw DexException("unsupported")
        }
      case j: Join =>
        // todo: turn left semi join to a subquery
        val leftSubquery = convertToSQL(j.left)
        val rightSubquery = convertToSQL(j.right)
        // Maybe a debugging hell to use natural join
        // alias for subqueries?
        s"($leftSubquery) NATURAL JOIN ($rightSubquery)"
      case c: CashTSelect =>
        val firstLabel = dialect.compileValue(s"${c.predicate}~1")
        val firstCounter = dialect.compileValue(1)
        s"""
           |WITH cash_select(value, counter) RECURSIVE AS (
           |  SELECT value, $firstCounter  FROM t_select WEHRE rid = $firstLabel
           |  UNION ALL
           |  SELECT t_select.value, cash_select.counter + 1 FROM cash_select, t_select
           |  WHERE cash_token(${c.predicate}, cash_select.counter + 1) = t_select.rid
           |)
           |SELECT value FROM cash_select
         """.stripMargin

    }

  }

  object UnresolveDexPlanAncestors extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.collectFirst { case p: DexPlan => p.outputSet } match {
        case Some(dexOutputSet) =>
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
        case None =>
          plan
      }
    }
  }

  object RemoveDexPlanNode extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transformDown {
        case d: DexPlan => d.child
      }
    }
  }

  /**
   *                                                           LeftSemi Join 1
   *                                                              /     \
   *                  LeftSemi Join N                  Data Relation   LeftSemi Join N
   *                      /     \                                            /    \
   *                    ...    Encrypted Filter N                          ...   Encrypted Filter N
   *                     /                                                  /
   *                LeftSemi Join 2                 ---->              LeftSemi Join 2
   *                  /        \                                            /       \
   *         LeftSemi Join 1  Encrypted Filter 2             Encrypted Filter 1  Encrypted Filter 2
   *          /        \
   *   Data Relation  Encrypted Filter 1
   *
   * Caveate: Each join comes with its own projection
   */
  object DelayDataTableLeftSemiJoinAfterFilters extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transformDown {
        case DataTableMultipleFilters(DataTableTwoFilters(_, _, (DataTableOneFilter(p1, j1, (d, _)), _)), lastFilterJoin) =>
          val filters = removeDataTableJoinIn(lastFilterJoin)
          val j1Condition = j1.condition.get.transform {
            case EqualTo(left: Attribute, right: Attribute) => EqualTo(d.resolve(Seq(left.name), resolver).get, filters.resolve(Seq(right.name), resolver).get)
          }
          Project(p1.projectList, Join(d, filters, LeftSemi, Some(j1Condition)))
      }
    }

    private def removeDataTableJoinIn(plan: LogicalPlan): LogicalPlan = plan.transformUp {
      case DataTableTwoFilters(_, j2, (DataTableOneFilter(_, _, (_, f1)), f2)) =>
        val j2Condition = j2.condition.get.transform {
          case EqualTo(left: Attribute, right: Attribute) => EqualTo(f1.resolve(Seq(left.name), resolver).get, f2.resolve(Seq(right.name), resolver).get)
        }
        Join(f1, f2, LeftSemi, Some(j2Condition))
    }

    object DataTableMultipleFilters extends PredicateHelper {
      type DataTableTwoFilters = LogicalPlan
      type LastFilterJoin = LogicalPlan
      def unapply(plan: LogicalPlan): Option[(DataTableTwoFilters, LastFilterJoin)] = plan match {
        case p @ DataTableTwoFilters(_, _, (_, _)) =>
          Some((p, p))
        case p @ Project(_, Join(left @ Join(_, _, LeftSemi, _), EncryptedFilterOperation(), LeftSemi, _)) =>
          unapply(left).map { case (d, _) => (d, p) }
        case _ => None
      }
    }

    object DataTableTwoFilters extends PredicateHelper {
      def unapply(plan: LogicalPlan): Option[(Project, Join, (LogicalPlan, LogicalPlan))] = plan match {
        case p @ Project(_, j @ Join(d @ DataTableOneFilter(_, _, (_, _)), f @ EncryptedFilterOperation(), LeftSemi, _)) =>
          Some((p, j, (d, f)))
        case _ => None
      }
    }

    object DataTableOneFilter extends PredicateHelper {
      def unapply(plan: LogicalPlan): Option[(Project, Join, (LogicalPlan, LogicalPlan))] = plan match {
        case p @ Project(_, j @ Join(d @ DataTableOperation(), f @ EncryptedFilterOperation(), LeftSemi, _)) =>
          Some((p, j, (d, f)))
        case _ => None
      }
    }

    object DataTableOperation extends PredicateHelper {
      def unapply(plan: LogicalPlan): Boolean = plan match {
        case Project(_, child: LogicalRelation) if !emmTables.contains(child) => true
        case _ => false
      }
    }

    object EncryptedFilterOperation extends PredicateHelper {
      def unapply(plan: LogicalPlan): Boolean = plan match {
        case Project(_, _: CashTSelect) => true
        case _ => false
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
        case l@LogicalRelation(relation: JDBCRelation, _, _, _) =>
          l.output.map(x => (x.exprId, relation.jdbcOptions.tableOrQuery))
      }.flatten.toMap

    private lazy val output = dexPlan.output.map(translateAttribute)

    def translate: LogicalPlan = analyze {
      val newPlan = translatePlan(dexPlan.child, None)
      newPlan.select(output: _*)
    }

    private def translateAttribute(attr: Attribute): NamedExpression = {
      Decrypt(metadataDecKey, attrEncOf(attr)).cast(attr.dataType).as(attr.name)
    }

    private def attrEncOf(attr: Attribute): Attribute = $"${attr.name}_prf"

    private def analyze(plan: LogicalPlan) =
      sparkSession.sessionState.analyzer.executeAndCheck(plan)

    private def translatePlan(plan: LogicalPlan, childView: Option[LogicalPlan]): LogicalPlan = analyze {
      plan match {
        case d: DexPlan =>
          // keep this annotation around
          translatePlan(d.child, childView)
        case l: LogicalRelation =>
          l.relation match {
            case j: JDBCRelation =>
              val tableName = j.jdbcOptions.tableOrQuery
              val tableEnc = tableEncOf(tableName)
              childView match {
                case Some(w) =>
                  w.join(tableEnc, NaturalJoin(LeftOuter))
                //w
                case None =>
                  tableEnc
              }
            case x => throw DexException("unsupported: " + x.toString)
          }

        case p: Project =>
          // todo: projection push down
          translatePlan(p.child, childView)

        case f: Filter =>
          val source = translatePlan(f.child, childView)
          translateFormula(FilterFormula, f.condition, source, isNegated = false)

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
            val joinView = translateFormula(JoinFormula, j.condition.get, leftView, isNegated = false)
            translatePlan(j.right, Some(joinView))
          }

        case x => throw DexException("unsupported: " + x.toString)
      }
    }

    private def nonIsNotNullPredsIn(conds: Seq[Expression]): AttributeSet = {
      conds.flatMap(_.collect {
        case x: BinaryComparison => x.references
      }).reduceOption(_ ++ _).getOrElse(AttributeSet.empty)
    }

    private def translateFormula(formulaType: FormulaType, condition: Expression, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = analyze {
      condition match {
        case p: EqualTo => formulaType match {
          case FilterFormula =>
            translateFilterPredicate(p, childView, isNegated)
          case JoinFormula =>
            translateJoinPredicate(p, childView)
        }

        case And(left, right) if !isNegated =>
          translateFormula(formulaType, right, translateFormula(formulaType, left, childView, isNegated), isNegated)

        case Or(left, right) if !isNegated =>
          val lt = translateFormula(formulaType, left, childView, isNegated)
          val rt = translateFormula(formulaType, right, childView, isNegated)
          //rt.flatMap(r => lt.map(l => l unionDistinct r)).orElse(lt)
          lt unionDistinct rt

        case IsNotNull(attr: Attribute) =>
          childView

        case In(attr: Attribute, list: Seq[Expression]) if formulaType == FilterFormula =>
          val pred = if (isNegated) {
            Not(list.map(expr => EqualTo(attr, expr)).reduce[Predicate]((p1, p2) => And(p1, p2)))
          } else {
            list.map(expr => EqualTo(attr, expr)).reduce[Predicate]((p1, p2) => Or(p1, p2))
          }
          translateFormula(formulaType, pred, childView, isNegated = false)

        case Not(p: Predicate) if formulaType == FilterFormula =>
          translateFormula(formulaType, p, childView, isNegated = true)

        case x => throw DexException("unsupported: " + x.toString)
      }
    }

    private def translateFilterPredicate(p: Predicate, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = p match {
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
        cashTSelectOf(tableName, predicate, ridOrder, childView, isNegated)

      case EqualTo(left: Attribute, right: Attribute) if left.name < right.name =>
        val colNames = (left.name, right.name)
        val tableNames = (exprIdToTable(left.exprId), exprIdToTable(right.exprId))
        require(tableNames._1 == tableNames._2)
        val tableName = tableNames._1
        val ridOrder = s"rid_${joinOrder(tableName)}"
        val predicate = s"$tableName~${colNames._1}~${colNames._2}"
        cashTSelectOf(tableName, predicate, ridOrder, childView, isNegated)

      case EqualTo(left: Attribute, right: Attribute) if left.name > right.name =>
        translateFilterPredicate(EqualTo(right, left), childView, isNegated)

      case x => throw DexException("unsupported: " + x.toString)
    }

    private def cashTSelectOf(predicateTableName: String, predicate: String, ridOrder: String, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = {
      val cashTSelect = CashTSelect(predicate, tSelect)
        .select(Decrypt(s"$emmDecKeyPrefix~$predicate", $"value").as(ridOrder))

      if (isNegated) {
        childView.join(cashTSelect, UsingJoin(LeftAnti, Seq(ridOrder)))
      } else {
        require(childView.output.exists(_.name == ridOrder))
        childView.join(cashTSelect, UsingJoin(LeftSemi, Seq(ridOrder)))
      }
    }

    private def translateJoinPredicate(p: Predicate, childView: LogicalPlan): LogicalPlan = p match {
      case EqualTo(left: Attribute, right: Attribute) =>
        val (leftColName, rightColName) = (left.name, right.name)
        val (leftTableName, rightTableName) = (exprIdToTable(left.exprId), exprIdToTable(right.exprId))
        val (leftRidOrder, rightRidOrder) = (s"rid_${joinOrder(leftTableName)}", s"rid_${joinOrder(rightTableName)}")
        val (leftRidOrderAttr, rightRidOrderAttr) = (
          childView.output.find(_.name == leftRidOrder),
          childView.output.find(_.name == rightRidOrder))

        val predicate = s"$leftTableName~$leftColName~$rightTableName~$rightColName"

        (leftRidOrderAttr, rightRidOrderAttr) match {
          case (Some(l), None) =>
            val cashTm = CashTM(predicate, childView, tM, l)
            val cashTmProject = cashTm.output.collect {
              case x: Attribute if x.name == "value" => Decrypt($"rid", x).as(rightRidOrder)
              case x: Attribute if x.name != "rid" => x
            }
            cashTm.select(cashTmProject: _*)

          case (Some(l), Some(r)) =>
            val cashTm = CashTM(predicate, childView, tM, l).where(EqualTo(r, Decrypt($"rid", $"value")))
            val cashTmProject = childView.output
            cashTm.select(cashTmProject: _*)

          case _ => ???
        }
      case _ => ???
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

    // TODO: use iterator to eliminate row copying. See ShuffledHashjoinExec.
    // If childView depends on a JDBCRDD (throuhg narrow dependency) then need to copy rows through JDBC cursors
    // before wide-dependency operations like join
    val childViewRdd = childView.execute().map(_.copy())

    // Copy emm rows through JDBC cursors before wide dependency operation like join below
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
