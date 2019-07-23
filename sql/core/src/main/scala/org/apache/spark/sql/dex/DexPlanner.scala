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
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, BinaryComparison, BinaryExpression, BindReferences, BoundReference, EqualTo, ExpectsInputTypes, ExprId, Expression, In, IsNotNull, JoinedRow, Literal, NamedExpression, Not, Or, Predicate, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

object DexConstants {
  val cashCounterStart: Int = 0
}

class DexPlanner(sessionCatalog: SessionCatalog, sparkSession: SparkSession) extends RuleExecutor[LogicalPlan] {

  private val tFilter = LogicalRelation(
    DataSource.apply(
      sparkSession,
      className = "jdbc",
      options = Map(
        JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
        JDBCOptions.JDBC_TABLE_NAME -> "t_filter")).resolveRelation())

  private val tCorrelatedJoin = LogicalRelation(
    DataSource.apply(
      sparkSession,
      className = "jdbc",
      options = Map(
        JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
        JDBCOptions.JDBC_TABLE_NAME -> "t_correlated_join")).resolveRelation())

  private val emmTables = Set(tFilter, tCorrelatedJoin)

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
    Batch("Preprocess Dex query", Once, UnresolveDexPlanAncestors),
    Batch("Translate Dex query", Once,
      TranslateDexQuery,
      DelayDataTableLeftSemiJoinAfterFilters
    ),
    Batch("Postprocess Dex query", Once,
      //RemoveDexPlanNode
      ConvertDexPlanToSQL
    )
  )

  private def analyze(plan: LogicalPlan) =
    sparkSession.sessionState.analyzer.executeAndCheck(plan)


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

   'Project [cast(dexdecrypt(metadata_dec_key, 'a_prf) as int) AS a#26, cast(dexdecrypt(metadata_dec_key, 'b_prf) as int) AS b#27, cast(dexdecrypt(metadata_dec_key, 'c_prf) as int) AS c#28, cast(dexdecrypt(metadata_dec_key, 'd_prf) as int) AS d#29]
+- 'DexPlan
   +- 'Join NaturalJoin(LeftOuter)
      :- 'Project ['rid_0, 'a_prf, 'b_prf, dexdecrypt('value_dec_key, value#16) AS rid_1#21]
      :  +- DexRidCorrelatedJoin testdata2~b~testdata3~c, rid_0#20: string
      :     :- Project [rid#17 AS rid_0#20, a_prf#18, b_prf#19]
      :     :  +- Relation[rid#17,a_prf#18,b_prf#19] JDBCRelation(testdata2_prf) [numPartitions=1]
      :     +- Relation[label#15,value#16] JDBCRelation(t_correlated_join) [numPartitions=1]
      +- Project [rid#22 AS rid_1#25, c_prf#23, d_prf#24]
         +- Relation[rid#22,c_prf#23,d_prf#24] JDBCRelation(testdata3_prf) [numPartitions=1]

         'Project [cast(dexdecrypt(metadata_dec_key, 'a_prf) as int) AS a#27, cast(dexdecrypt(metadata_dec_key, 'b_prf) as int) AS b#28, cast(dexdecrypt(metadata_dec_key, 'c_prf) as int) AS c#29, cast(dexdecrypt(metadata_dec_key, 'd_prf) as int) AS d#30]
+- 'DexPlan
   +- 'Join NaturalJoin(LeftOuter)
      :- 'Project ['rid_0, 'a_prf, 'b_prf, 'rid_0, dexdecrypt('value_dec_key, value#16) AS rid_1#22]
      :  +- 'DexRidCorrelatedJoin testdata2~b~testdata3~c, rid_0#20: string
      :     :- 'Join UsingJoin(LeftSemi,List(rid_0))
      :     :  :- Project [rid#17 AS rid_0#20, a_prf#18, b_prf#19]
      :     :  :  +- Relation[rid#17,a_prf#18,b_prf#19] JDBCRelation(testdata2_prf) [numPartitions=1]
      :     :  +- 'Project [dexdecrypt('value_dec_key, 'value) AS rid_0#21]
      :     :     +- DexRidFilter testdata2~a~2
      :     :        +- Relation[label#13,value#14] JDBCRelation(t_filter) [numPartitions=1]
      :     +- Relation[label#15,value#16] JDBCRelation(t_correlated_join) [numPartitions=1]
      +- Project [rid#23 AS rid_1#26, c_prf#24, d_prf#25]
         +- Relation[rid#23,c_prf#24,d_prf#25] JDBCRelation(testdata3_prf) [numPartitions=1]
   */
  object ConvertDexPlanToSQL extends Rule[LogicalPlan] {

    private val genSubqueryName = "__dex_gen_subquery_name"
    private val curId = new java.util.concurrent.atomic.AtomicLong(0L)

    private val dialect = JdbcDialects.get(JDBCOptions.JDBC_URL)

    private val jdbcRelationProvider = DataSource.lookupDataSource("jdbc", sparkSession.sqlContext.conf).newInstance().asInstanceOf[JdbcRelationProvider]

    override def apply(plan: LogicalPlan): LogicalPlan = {
      log.warn("== To be converted to sql ==\n" + plan.treeString(verbose = true))
      plan.transformDown {
        case unresolved: DexPlan =>
          // Don't resolve the ancestors of DexPlan, because they need to be resolve once the new LogicalRelation
          // for the DexPlan SQL has been resolved.
          // analyze DexPlan to resolve its output attributes.  Their dataTypes are needed for creating LogicalRelation
          val p = analyze(unresolved)
          val sql = convertToSQL(p)
          val jdbcParams = Map(
            JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
            JDBCOptions.JDBC_QUERY_STRING -> sql)
          val baseRelation = jdbcRelationProvider.createRelation(sparkSession.sqlContext, jdbcParams)
          /*val jdbcOption = new JDBCOptions(Map(
            JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
            JDBCOptions.JDBC_QUERY_STRING -> sql))
          val baseRelation = JDBCRelation(p.schema, Array.empty, jdbcOption)(sparkSession)*/
          LogicalRelation(baseRelation)
      }
    }

    private def convertToSQL(plan: LogicalPlan): String = plan match {
      case p: DexPlan => convertToSQL(p.child)
      case p: Project =>
        val projectList = p.projectList.map {
          case Alias(d: DexDecrypt, name) =>
            val decValue = decryptCol(d.left.dialectSql(dialect.quoteIdentifier), d.right.dialectSql(dialect.quoteIdentifier))
            s"$decValue AS ${dialect.quoteIdentifier(name)}"
          case x =>
            x.dialectSql(dialect.quoteIdentifier)
        } mkString ", "
        s"""
           |SELECT $projectList
           |FROM ${convertToSQL(p.child)}
          """.stripMargin
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
        s"""($leftSubquery) AS ${generateSubqueryName()}
           |
           |NATURAL JOIN
           |
           |($rightSubquery) AS ${generateSubqueryName()}
          """.stripMargin
      case f: DexRidFilter =>
        val (labelPrfKey, valueDecKey) = emmKeys(f.predicate)
        val firstLabel = nextLabel(labelPrfKey, s"${DexConstants.cashCounterStart}")
        val outputCols = f.output.map(_.dialectSql(dialect.quoteIdentifier)).mkString(", ")
        val emm = dialect.quoteIdentifier(tFilter.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        s"""
           |(
           |  WITH RECURSIVE dex_rid_filter(value_dec_key, value, counter) AS (
           |    SELECT $valueDecKey, $emm.value, ${DexConstants.cashCounterStart}  FROM $emm WHERE label = $firstLabel
           |    UNION ALL
           |    SELECT $valueDecKey, $emm.value, dex_rid_filter.counter + 1 FROM dex_rid_filter, $emm
           |    WHERE ${nextLabel(labelPrfKey, "dex_rid_filter.counter + 1")} = $emm.label
           |  )
           |  SELECT $outputCols FROM dex_rid_filter
           |) AS ${generateSubqueryName()}
         """.stripMargin
      case j: DexRidCorrelatedJoin =>
        val leftSubquery = convertToSQL(j.left)
        val leftRid = j.childViewRid.dialectSql(dialect.quoteIdentifier)
        val (labelPrfKey, valueDecKey) = emmKeysOfRidCol(leftRid, j.predicate)
        val emm = dialect.quoteIdentifier(tCorrelatedJoin.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        val outputCols = j.outputSet.map(_.dialectSql(dialect.quoteIdentifier)).mkString(", ")
        s"""
           |(
           |  WITH RECURSIVE left_subquery_all_cols AS (
           |    SELECT * FROM ($leftSubquery) AS ${generateSubqueryName()}
           |  ),
           |  left_subquery($leftRid, label_prf_key, value_dec_key) AS(
           |    SELECT $leftRid, $labelPrfKey, $valueDecKey FROM left_subquery_all_cols
           |  ),
           |  dex_rid_correlated_join($leftRid, label_prf_key, value_dec_key, value, counter) AS (
           |    SELECT left_subquery.*, $emm.value, ${DexConstants.cashCounterStart} AS counter
           |    FROM left_subquery, $emm
           |    WHERE $emm.label =
           |      ${nextLabel("label_prf_key", s"${DexConstants.cashCounterStart}")}
           |
           |    UNION ALL
           |
           |    SELECT $leftRid, label_prf_key, value_dec_key, $emm.value, counter + 1 AS counter
           |    FROM dex_rid_correlated_join, $emm
           |    WHERE $emm.label = ${nextLabel("label_prf_key", "counter + 1")}
           |  )
           |  SELECT $outputCols FROM dex_rid_correlated_join NATURAL JOIN left_subquery_all_cols
           |) AS ${generateSubqueryName()}
         """.stripMargin

    }

    private def emmKeysOfRidCol(ridCol: String, predicate: String): (String, String) = {
      // todo: append 1 and 2 to form two keys
      val predRidCol = s"'$predicate' || '~' || $ridCol"
      (predRidCol, predRidCol)
    }

    private def emmKeys(predicate: String): (String, String) =
      // todo: append 1 and 2 to form two keys
      (dialect.compileValue(predicate).asInstanceOf[String],
        dialect.compileValue(predicate).asInstanceOf[String])

    private def nextLabel(labelPrfKey: String, nextCounter: String): String =
      s"$labelPrfKey || ${dialect.compileValue("~")} || $nextCounter"

    private def decryptCol(decKey: String, col: String): String =
      // todo: use SQL decrypt like s"decrypt($decKey, $col)"
      s"substring($col, '(.+)_enc$$')"

    private def generateSubqueryName() =
      s"${genSubqueryName}_${curId.getAndIncrement()}"

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
        case Project(_, _: DexRidFilter) => true
        case _ => false
      }
    }
  }

  object TranslateDexQuery extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan transformUp {
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

    def translate: LogicalPlan = {
      // We want to preserve the DexPlan nodes during the translation process because we will use them to determine
      // boundaries for non-Dex and Dex queries for SQL conversion later.
      // But we need to be careful when preserving the DexPlan due to recursive transformation of the tree.
      // There would be trouble if we did 'val newPlan = tranlsatePlan(dexPlan, None)' i.e. let the translatePlan()
      // to add back the DexPlan, because translatePlan() is recursive and it would have a hard time to differentiate
      // the root DexPlan to preserve and any subtree DexPlan (already translated, hence to ignore).
      // So we have to add back the root DexPlan here to avoid the ambiguity.
      val newPlan = DexPlan(translatePlan(dexPlan.child, None))
      newPlan.select(output: _*)
    }

    private def translateAttribute(attr: Attribute): NamedExpression = {
      DexDecrypt(metadataDecKey, attrEncOf(attr)).cast(attr.dataType).as(attr.name)
    }

    private def attrEncOf(attr: Attribute): Attribute = $"${attr.name}_prf"

    private def translatePlan(plan: LogicalPlan, childView: Option[LogicalPlan]): LogicalPlan = {
      plan match {
        case d: DexPlan =>
          // Because we're transforming up the tree, we may encounter subtree DexPlan that has already been translated.
          // So here we simply return it as is
          d
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

    private def translateFormula(formulaType: FormulaType, condition: Expression, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = {
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
        dexFilterOf(tableName, predicate, ridOrder, childView, isNegated)

      case EqualTo(left: Attribute, right: Attribute) if left.name < right.name =>
        val colNames = (left.name, right.name)
        val tableNames = (exprIdToTable(left.exprId), exprIdToTable(right.exprId))
        require(tableNames._1 == tableNames._2)
        val tableName = tableNames._1
        val ridOrder = s"rid_${joinOrder(tableName)}"
        val predicate = s"$tableName~${colNames._1}~${colNames._2}"
        dexFilterOf(tableName, predicate, ridOrder, childView, isNegated)

      case EqualTo(left: Attribute, right: Attribute) if left.name > right.name =>
        translateFilterPredicate(EqualTo(right, left), childView, isNegated)

      case x => throw DexException("unsupported: " + x.toString)
    }

    private def dexFilterOf(predicateTableName: String, predicate: String, ridOrder: String, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = {
      val ridFilter = DexRidFilter(predicate, tFilter)
        .select(DexDecrypt($"value_dec_key", $"value").as(ridOrder))

      if (isNegated) {
        childView.join(ridFilter, UsingJoin(LeftAnti, Seq(ridOrder)))
      } else {
        require(childView.output.exists(_.name == ridOrder))
        childView.join(ridFilter, UsingJoin(LeftSemi, Seq(ridOrder)))
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
            // "right" relation is a new relation to join
            val ridJoin = DexRidCorrelatedJoin(predicate, childView, tCorrelatedJoin, l)
            val ridJoinProject = ridJoin.output.collect {
              case x: Attribute if x.name == "value" => DexDecrypt($"value_dec_key", x).as(rightRidOrder)
              case x: Attribute if x.name != "value_dec_key" => // remove extra value_dec_key column
                // Unresolve all but the emm attributes.  This is overshooting a bit, because we only care about
                // the case for natural joining the base table for joining attributes (rid_1#33, rid_1#90),
                // but the optimizer will insert a new project node on top of this join and only takes one of the
                // join columns, say rid_1#33.  This step happens AFTER DexPlan translation, so to go around this
                // we need to unresolve all the output attributes from the existing projection to allow
                // later on resolution onto the new project.
                UnresolvedAttribute(x.name)
            }
            // Need to deduplicate ridJoinProject because left subquery might have the same attribute name
            // as the right subquery, such as simple one filter one join case where rid_0 are from both
            // the filter operator and the join operator.
            ridJoin.select(ridJoinProject.distinct: _*)

          case (Some(l), Some(r)) =>
            // "right" relation is a previously joined relation
            // don't have extra "value_dec_key" column
            val ridJoin = DexRidCorrelatedJoin(predicate, childView, tCorrelatedJoin, l).where(EqualTo(r, DexDecrypt($"value_dec_key", $"value")))
            val ridJoinProject = childView.output
            ridJoin.select(ridJoinProject: _*)

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

case class DexDecrypt(key: Expression, value: Expression) extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

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

case class DexRidFilterExec(predicate: String, emm: SparkPlan) extends UnaryExecNode {

  private val labelForCounter: Int => InternalRow => Boolean = {
    counter => emmRow => {
      val lhs = UTF8String.fromString(s"$predicate~$counter")
      val emmLabelCol = BindReferences.bindReference(emm.output.head, emm.output).asInstanceOf[BoundReference]
      val ordering = TypeUtils.getInterpretedOrdering(emmLabelCol.dataType)
      val rhs = emmLabelCol.eval(emmRow)
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
    Iterator.from(DexConstants.cashCounterStart).map { i =>
      emmRdd.mapPartitionsInternal { emmIter =>
        emmIter.find(labelForCounter(i)).iterator
      }
    }.takeWhile(!_.isEmpty()).reduceOption(_ ++ _).getOrElse(sparkContext.emptyRDD)
  }

  override def output: Seq[Attribute] = emm.output.collect {
    case x: Attribute if x.name == "label" =>
      // rename column "label"
      x.withName("value_dec_key")
    case x => x
  }

  /*override def requiredChildDistribution: Seq[Distribution] = emmType match {
    case EmmTSelect =>
       UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
    case _ => ???
  }*/

  override def child: SparkPlan = emm
}

case class DexRidCorrelatedJoinExec(predicate: String, childView: SparkPlan, emm: SparkPlan, childViewRid: Attribute) extends BinaryExecNode {
  override def left: SparkPlan = childView

  override def right: SparkPlan = emm

  /**
    * Produces the result of the query as an `RDD[InternalRow]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalRow] = {
    val childViewRidCol = BindReferences.bindReference(childViewRid, childView.output).asInstanceOf[BoundReference]
    val emmLabelCol = BindReferences.bindReference(emm.output.head, emm.output).asInstanceOf[BoundReference]
    val emmValueCol = BindReferences.bindReference(emm.output.apply(1), emm.output).asInstanceOf[BoundReference]

    // TODO: use iterator to eliminate row copying. See ShuffledHashjoinExec.
    // If childView depends on a JDBCRDD (throuhg narrow dependency) then need to copy rows through JDBC cursors
    // before wide-dependency operations like join
    val childViewRdd = childView.execute().map(_.copy())

    // Copy emm rows through JDBC cursors before wide dependency operation like join below
    val emmRdd =
      emm.execute().map(row => (emmLabelCol.eval(row).asInstanceOf[UTF8String], row.copy()))

    var childViewRddToCount = childViewRdd
    Iterator.from(DexConstants.cashCounterStart).map { i =>
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

  override def output: Seq[Attribute] = left.output ++ (right.output.collect {
    case x: Attribute if x.name == "label" =>
      // rename column "label"
      x.withName("value_dec_key")
    case x => x
  })
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
