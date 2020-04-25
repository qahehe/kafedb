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
import org.apache.spark.sql.catalyst.dex.{DexConstants, DexDecode, DexDecrypt, DexException, DexPrimitives}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Add, And, Attribute, AttributeSet, BinaryComparison, BinaryExpression, BindReferences, BoundReference, Concat, DialectSQLTranslatable, EqualTo, ExpectsInputTypes, ExprId, Expression, In, IsNotNull, Literal, NamedExpression, Not, Or, Predicate, PredicateHelper, PrettyAttribute, SqlDialect}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.catalyst.dex.DexConstants.{AttrName, TableAttributeAtom, TableAttributeCompound, ridCol}
import org.apache.spark.sql.catalyst.dex.DexPrimitives._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{AtomicType, DataType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{Column, SparkSession, dex}
import org.apache.spark.unsafe.types.UTF8String

class DexPlanner(sessionCatalog: SessionCatalog, sparkSession: SparkSession) extends RuleExecutor[LogicalPlan] {

  def emmTableOf(name: String): LogicalRelation = {
    LogicalRelation(
      DataSource.apply(
        sparkSession,
        className = "jdbc",
        options = Map(
          JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
          JDBCOptions.JDBC_TABLE_NAME -> name)).resolveRelation())
  }

  private val sqlConf = SQLConf.get

  private lazy val tFilter = emmTableOf(DexConstants.tFilterName)

  private lazy val tDepFilter = emmTableOf(DexConstants.tDepFilterName)

  private lazy val tCorrelatedJoin = emmTableOf(DexConstants.tCorrJoinName)

  private lazy val tUncorrelatedJoin = emmTableOf(DexConstants.tUncorrJoinName)

  private lazy val tDomain = emmTableOf(DexConstants.tDomainName)

  private lazy val emmTables = Set(tFilter, tCorrelatedJoin, tUncorrelatedJoin, tDomain)

  private val resolver = sparkSession.sqlContext.conf.resolver

  //val decryptValueToRid = functions.udf((value: String) => """(.+)_enc$""".r.findFirstIn(value).get)
  //sparkSession.udf.register("decryptValueToRid", decryptValueToRid)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = Seq(
    // todo first need to move/coallese the DexPlan operators
    Batch("Preprocess Dex query", Once, UnresolveDexPlanAncestors),
    Batch("Translate Dex query", Once,
      TranslateDexQuery
      //DelayDataTableLeftSemiJoinAfterFilters
    ),
    Batch("Postprocess Dex query", Once,
      //RemoveDexPlanNode
      ConvertDexPlanToSQL
    )
  )

  private def analyze(plan: LogicalPlan): LogicalPlan =
    if (plan.resolved)
      plan
    else
      sparkSession.sessionState.analyzer.executeAndCheck(plan)

  private def resolvedAttribute(attr: UnresolvedAttribute, plan: LogicalPlan): Attribute =
    plan.output.find(_.name.contains(attr.name)).get

  private def isTableScan(view: LogicalPlan): Boolean =
    (view.isInstanceOf[Project] && view.children.length == 1 && view.children.head.isInstanceOf[LogicalRelation]) ||
      isTableName(view)

  private def isTableName(view: LogicalPlan): Boolean =
    view.children.isEmpty && view.isInstanceOf[LogicalRelation]

  private def isProject(view: LogicalPlan): Boolean =
    view.isInstanceOf[Project]

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
        val projectList = p.projectList.map(_.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect)).mkString(", ")
        val childSql = if (isTableName(p.child)) convertToSQL(p.child) else s"(${convertToSQL(p.child)}) AS ${generateSubqueryName()}"
        s"""
           |SELECT $projectList
           |FROM $childSql
          """.stripMargin
      case p: LogicalRelation =>
        p.relation match {
          case j: JDBCRelation => j.jdbcOptions.tableOrQuery
          case _ => throw DexException("unsupported")
        }
      case f: Filter =>
        // This is a regular filter, added for example for the case of join partially coincide with filters
        val childSql = if (isTableName(f.child)) convertToSQL(f.child) else s"(${convertToSQL(f.child)}) AS ${generateSubqueryName()}"
        s"""
           |SELECT * FROM
           |$childSql
           |WHERE ${f.condition.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect)}
         """.stripMargin
      case j: Join =>
        val leftSubquery = if (isTableName(j.left)) convertToSQL(j.left) else s"(${convertToSQL(j.left)}) AS ${generateSubqueryName()}"
        val rightSubquery = if (isTableName(j.right)) convertToSQL(j.right) else s"(${convertToSQL(j.right)}) AS ${generateSubqueryName()}"
        j.joinType match {
          case x if x == Cross  =>
            s"""
               |$leftSubquery
               |
               |CROSS JOIN
               |
               |$rightSubquery
              """.stripMargin
          case x if x == Inner && isNaturalJoin(j) =>
            s"""
               |$leftSubquery
               |
               |NATURAL INNER JOIN
               |
               |$rightSubquery
              """.stripMargin
          case x if x == Inner && j.condition.isDefined =>
            s"""
               |$leftSubquery
               |
               |INNER JOIN
               |
               |$rightSubquery
               |
               |ON (${j.condition.get.asInstanceOf[DialectSQLTranslatable].dialectSql(dialect)})
              """.stripMargin
          case x if x == LeftSemi && j.condition.isDefined =>
            val (leftRids, rightRids) = ridOrdersFromJoin(j)
            s"""
               |SELECT * FROM
               |$leftSubquery
               |WHERE ($leftRids) IN (
               |  SELECT $rightRids
               |  FROM $rightSubquery
               |)
             """.stripMargin
          case x if x == RightSemi && j.condition.isDefined =>
            val (leftRids, rightRids) = ridOrdersFromJoin(j)
            s"""
               |SELECT * FROM
               |$rightSubquery
               |WHERE ($rightRids) IN (
               |  SELECT $leftRids
               |  FROM $leftSubquery
               |)
             """.stripMargin
          case x if x == LeftAnti && j.condition.isDefined =>
            val (leftRid, rightRid) = ridOrdersFromJoin(j)
            s"""
               |SELECT * FROM
               |$leftSubquery
               |WHERE ($leftRid) NOT IN (
               |  SELECT $rightRid
               |  FROM $rightSubquery
               |)
             """.stripMargin
          case x if x == RightOuter && isNaturalJoin(j) =>
            s"""
               |$leftSubquery
               |
               |NATURAL RIGHT OUTER JOIN
               |
               |$rightSubquery
              """.stripMargin
          case x => throw DexException("unsupported: " + x.getClass.getName + " in join: " + j.toString)
        }

      case i: Intersect =>
        val leftSubquery = if (isTableName(i.left)) convertToSQL(i.left) else s"(${convertToSQL(i.left)}) AS ${generateSubqueryName()}"
        val rightSubquery = if (isTableName(i.right)) convertToSQL(i.right) else s"(${convertToSQL(i.right)}) AS ${generateSubqueryName()}"
        val intersectClause = if (i.isAll) "INTERSECT ALL" else "INTERSECT"
        s"""
           |$leftSubquery
           |
           |$intersectClause
           |
           |$rightSubquery
         """.stripMargin
      case Distinct(Union(left :: right :: Nil)) =>
        val leftSubquery = if (isTableName(left)) convertToSQL(left) else s"(${convertToSQL(left)}) AS ${generateSubqueryName()}"
        val rightSubquery = if (isTableName(right)) convertToSQL(right) else s"(${convertToSQL(right)}) AS ${generateSubqueryName()}"
        s"""
           |$leftSubquery
           |
           |UNION
           |
           |$rightSubquery
         """.stripMargin

      case f: DexRidFilter =>
        val (labelPrfKeyExpr, valueDecKeyExpr) = (
          Literal(dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, f.predicate, 1)),
          Literal(dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, f.predicate, 2))
        )
        val firstLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, Literal(DexConstants.cashCounterStart)).dialectSql(dialect)
        val nextLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, Add($"dex_rid_filter.counter", 1)).dialectSql(dialect)
        val outputCols = f.output.map(_.dialectSql(dialect)).mkString(", ")
        val emm = dialect.quoteIdentifier(tFilter.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        s"""
           |  WITH RECURSIVE dex_rid_filter(value_dec_key, value, counter) AS (
           |    SELECT ${valueDecKeyExpr.dialectSql(dialect)}, $emm.value, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} FROM $emm WHERE label = $firstLabelExpr
           |    UNION ALL
           |    SELECT ${valueDecKeyExpr.dialectSql{dialect}}, $emm.value, dex_rid_filter.counter + 1 FROM $emm, dex_rid_filter
           |    WHERE $emm.label = $nextLabelExpr
           |  )
           |  SELECT $outputCols FROM dex_rid_filter
         """.stripMargin
      case j: SpxRidUncorrelatedJoin =>
        val (labelPrfKeyExpr, valueDecKeyExpr) = (
          Literal(dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, j.predicate, 1)),
          Literal(dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, j.predicate, 2))
        )
        val firstLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, DexConstants.cashCounterStart).dialectSql(dialect)
        val nextLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, Add($"spx_rid_uncorrelated_join.counter", 1)).dialectSql(dialect)
        val outputCols = j.output.map(_.dialectSql(dialect)).mkString(", ")
        val emm = dialect.quoteIdentifier(tUncorrelatedJoin.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        s"""
           |  WITH RECURSIVE spx_rid_uncorrelated_join(value_dec_key, value_left, value_right, counter) AS (
           |    SELECT ${valueDecKeyExpr.dialectSql(dialect)}, $emm.value_left, $emm.value_right, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}  FROM $emm WHERE label = $firstLabelExpr
           |    UNION ALL
           |    SELECT ${valueDecKeyExpr.dialectSql(dialect)}, $emm.value_left, $emm.value_right, spx_rid_uncorrelated_join.counter + 1 FROM $emm, spx_rid_uncorrelated_join
           |    WHERE $emm.label = $nextLabelExpr
           |  )
           |  SELECT $outputCols FROM spx_rid_uncorrelated_join
         """.stripMargin
      case j: DexRidCorrelatedJoin =>
        val leftSubquery = if (isTableName(j.left)) convertToSQL(j.left) else s"(${convertToSQL(j.left)}) AS ${generateSubqueryName()}"
        val leftRidExpr = j.childViewRid
        val masterTrapdoor = dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, j.predicatePrefix)
        //val joinPredExpr = catalystConcatPredicateExprsOf(j.predicatePrefix, leftRidExpr)
        val (labelPrfKeyExpr, valueDecKeyExpr) = (
          catalystTrapdoorExprOf(Literal(masterTrapdoor), leftRidExpr, 1),
          catalystTrapdoorExprOf(Literal(masterTrapdoor), leftRidExpr, 2)
        )
        val firstLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, DexConstants.cashCounterStart).dialectSql(dialect)
        val nextLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, Add($"counter", 1)).dialectSql(dialect)
        val emm = dialect.quoteIdentifier(tCorrelatedJoin.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        val outputCols = j.outputSet.map(_.dialectSql(dialect)).mkString(", ")
        val leftSubqueryOutputCols = j.left.outputSet.map(_.dialectSql(dialect)).mkString(", ")

        // Semantically what we want is that for each (unique) leftRidExpr to join in leftSubquery, find out what are the
        // rightRid to join in t_correlated_join, and for the rows that are already associated with leftRidExpr, copy
        // them exactly X times where X = join size of leftRidExpr join rightRid.
        // But because the emm part is done using recursion, within the recursion, typical SQL does not allow
        // "project all columns, replacing the one called counter with counter + 1 and name it counter "
        // So we need to select distinct leftRidExpr in left_subquery and once generated all join pairs of leftRidExpr and rightRid,
        // join the result back with rows associated with leftRidExpr in left_subquery_all_cols.
        // If we can do the row projection replacement, then we can also express the computation without the lastly natural
        // join with left_sbuquery_all_cols, but potentially with duplicate calls to t_correlated_join.
        /*s"""
           |(
           |  WITH RECURSIVE left_subquery_all_cols AS (
           |   $leftSubquery
           |  ),
           |  left_subquery($leftRidExpr, label_prf_key, value_dec_key) AS(
           |    SELECT distinct $leftRidExpr, $labelPrfKeyExpr, ${valueDecKeyExpr.dialectSql(dialect)} FROM left_subquery_all_cols
           |  ),
           |  dex_rid_correlated_join($leftRidExpr, label_prf_key, value_dec_key, value, counter) AS (
           |    SELECT left_subquery.*, $emm.value, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM left_subquery, $emm
           |    WHERE $emm.label =
           |      ${nextLabel("label_prf_key", s"${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}")}
           |
           |    UNION ALL
           |
           |    SELECT $leftRidExpr, label_prf_key, value_dec_key, $emm.value, counter + 1 AS counter
           |    FROM dex_rid_correlated_join, $emm
           |    WHERE $emm.label = ${nextLabel("label_prf_key", "counter + 1")}
           |  )
           |  SELECT $outputCols FROM dex_rid_correlated_join NATURAL JOIN left_subquery_all_cols
           |) AS ${generateSubqueryName()}
         """.stripMargin*/


        s"""
           |  WITH RECURSIVE dex_rid_correlated_join AS (
           |    SELECT $leftSubqueryOutputCols, ${valueDecKeyExpr.dialectSql(dialect)} AS value_dec_key, $emm.value AS value, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM $leftSubquery, $emm
           |    WHERE $emm.label = $firstLabelExpr
           |
           |    UNION ALL
           |
           |    SELECT $leftSubqueryOutputCols, ${valueDecKeyExpr.dialectSql(dialect)} AS value_dec_key, $emm.value AS value, counter + 1 AS counter
           |    FROM dex_rid_correlated_join, $emm
           |    WHERE $emm.label = $nextLabelExpr
           |  )
           |  SELECT $outputCols FROM dex_rid_correlated_join
         """.stripMargin

      case v: DexDomainValues =>
        val (labelPrfKeyExpr, valueDecKeyExpr) = (
          Literal(dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, v.predicate, 1)),
          Literal(dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, v.predicate, 2))
        )
        val firstLabel = catalystEmmLabelExprOf(labelPrfKeyExpr, Literal(DexConstants.cashCounterStart))
        val outputCols = v.output.map(_.dialectSql(dialect)).mkString(", ")
        val emm = dialect.quoteIdentifier(tDomain.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        s"""
           |(
           |  WITH RECURSIVE dex_domain_values(value_dec_key, value, counter) AS (
           |    SELECT ${valueDecKeyExpr.dialectSql(dialect)}, $emm.value, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} FROM $emm WHERE label = $firstLabel
           |    UNION ALL
           |    SELECT ${valueDecKeyExpr.dialectSql(dialect)}, $emm.value, dex_domain_values.counter + 1 FROM dex_domain_values, $emm
           |    WHERE ${catalystEmmLabelExprOf(labelPrfKeyExpr, "dex_domain_values.counter + 1")} = $emm.label
           |  )
           |  SELECT $outputCols FROM dex_domain_values
           |) AS ${generateSubqueryName()}
         """.stripMargin
      /*case r: DexDomainRids =>
        // todo: Don't want to pass on emm keys on plaintext predicate to the server because it would then know how to
        // compose predicates by itself.  Instead, we do two-hop prf on plaintext predicate and only let the server
        // know the second hop and its key
        val domainValueSubquery = convertToSQL(r.domainValues)
        val domainValueExpr = r.domainValueAttr.dialectSql(dialect.quoteIdentifier)
        val domainValuePredExpr = dbConcatPredicateExprsOf(r.predicatePrefix, domainValueExpr)
        val (labelPrfKeyExpr, valueDecKeyExpr) = (
          dbEmmLabelPrfKeyExprOf(domainValuePredExpr),
          dbEmmValueEncKeyExprOf(domainValuePredExpr)
        )
        val emm = dialect.quoteIdentifier(tFilter.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        val outputCols = r.outputSet.map(_.dialectSql(dialect.quoteIdentifier)).mkString(", ")
        s"""
           |(
           |  WITH RECURSIVE dex_domain_values_keys($domainValueExpr, label_prf_key, value_dec_key) AS (
           |    SELECT $domainValueExpr, $labelPrfKeyExpr, ${valueDecKeyExpr.dialectSql(dialect)} FROM ($domainValueSubquery)
           |  ),
           |  dex_domain_rids($domainValueExpr, label_prf_key, value_dec_key, value, counter) AS (
           |    SELECT dex_domain_values_keys.*, $emm.value, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM dex_domain_values_keys, $emm
           |    WHERE $emm.label =
           |      ${dbEmmLabelExprOf("label_prf_key", s"${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}")}
           |    UNION ALL
           |    SELECT $domainValueExpr, label_prf_key, value_dec_key, $emm.value, counter + 1 AS counter
           |    FROM dex_domain_rids, $emm
           |    WHERE $emm.label = ${dbEmmLabelExprOf("label_prf_key", "counter + 1")}
           |  )
           |  SELECT $outputCols FROM dex_domain_rids
           |) AS ${generateSubqueryName()}
         """.stripMargin*/
      case j: DexDomainJoin =>
        val domainValueExpr = dialect.quoteIdentifier("value_dom")
        val emm = dialect.quoteIdentifier(tFilter.relation.asInstanceOf[JDBCRelation].jdbcOptions.tableOrQuery)
        // do not use outputSet, because dexOperators renaming columns using "withName" would still be distinguished
        // using the old names.
        val outputCols = j.output.map(_.dialectSql(dialect)).mkString(", ")

        val intersectedDomainValueSubquery =
          s"""
             |  dex_intersected_domain_values($domainValueExpr) AS (
             |    ${convertToSQL(j.intersectedDomainValues)}
             |  )
          """.stripMargin

        def ctePartFor(joinSide: String, predicatePrefix: String) = {
          //val predicatePrefixExpr = dialect.compileValue(predicatePrefix).asInstanceOf[String]
          //val domainValuePredExpr = catalystConcatPredicateExprsOf(predicatePrefixExpr, domainValueExpr)
          val masterTrapdoor = dexTrapdoorForPred(masterSecret.hmacKey.getEncoded, predicatePrefix)
          val (labelPrfKeyExpr, valueDecKeyExpr) = (
            catalystTrapdoorExprOf(Literal(masterTrapdoor), domainValueExpr, 1),
            catalystTrapdoorExprOf(Literal(masterTrapdoor), domainValueExpr, 2)
          )
          s"""
             |  dex_domain_values_keys_$joinSide($domainValueExpr, label_prf_key_$joinSide, value_dec_key_$joinSide) AS (
             |    SELECT $domainValueExpr, $labelPrfKeyExpr, ${valueDecKeyExpr.dialectSql(dialect)} FROM dex_intersected_domain_values
             |  ),
             |  dex_domain_rids_$joinSide($domainValueExpr, label_prf_key_$joinSide, value_dec_key_$joinSide, value_$joinSide, counter_$joinSide) AS (
             |    SELECT dex_domain_values_keys_$joinSide.*, $emm.value, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter_$joinSide
             |    FROM dex_domain_values_keys_$joinSide, $emm
             |    WHERE $emm.label =
             |      ${catalystEmmLabelExprOf(s"label_prf_key_$joinSide", s"${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}")}
             |    UNION ALL
             |    SELECT $domainValueExpr, label_prf_key_$joinSide, value_dec_key_$joinSide, $emm.value, counter_$joinSide + 1 AS counter_$joinSide
             |    FROM dex_domain_rids_$joinSide, $emm
             |    WHERE $emm.label = ${catalystEmmLabelExprOf(s"label_prf_key_$joinSide", s"counter_$joinSide + 1")}
             |  )
         """.stripMargin
        }

        s"""
           |(
           |  WITH RECURSIVE
           |  $intersectedDomainValueSubquery,
           |  ${ctePartFor("left", j.leftPredicate)},
           |  ${ctePartFor("right", j.rightPredicate)}
           |  SELECT $outputCols FROM dex_domain_rids_left NATURAL JOIN dex_domain_rids_right
           |) AS ${generateSubqueryName()}
         """.stripMargin

      case f: DexPseudoPrimaryKeyDependentFilter =>
        // val predExpr = dialect.compileValue(f.predicate).asInstanceOf[String]
        // val labelPrfKeyExpr = Literal(dexTrapdoor(masterSecret.hmacKey.getEncoded, predExpr))
        val labelPrfKeyExpr = Literal(dexMasterTrapdoorForPred(f.predicate, None))
        val labelColExpr = dialect.quoteIdentifier(f.labelColumn)
        val outputCols = f.output.map(_.dialectSql(dialect)).mkString(", ")
        val ridOrder = f.filterTableRid.dialectSql(dialect)
        val firstLabelExpr = catalystEmmLabelExprOf(
          labelPrfKeyExpr, DexConstants.cashCounterStart
        ).dialectSql(dialect)
        val nextLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, Add($"dex_ppk_filter.counter", 1)).dialectSql(dialect)

        /*s"""
           |(
           |  WITH RECURSIVE child_view AS (
           |    ${convertToSQL(f.child)}
           |  ),
           |  t(counter, label) AS (
           |    SELECT ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}, ${nextLabel(labelPrfKeyExpr, DexConstants.cashCounterStart.toString)} WHERE EXISTS (SELECT 1 FROM child_view WHERE ${nextLabel(labelPrfKeyExpr, DexConstants.cashCounterStart.toString)} = $labelColExpr)
           |    UNION ALL
           |    SELECT counter + 1, ${nextLabel(labelPrfKeyExpr, "counter + 1")} FROM t WHERE EXISTS (SELECT 1 FROM child_view WHERE label = $labelColExpr)
           |  )
           |  SELECT $outputCols FROM t INNER JOIN child_view ON label = $labelColExpr
           |)
         """.stripMargin*/
        /*s"""
           |(
           |  WITH RECURSIVE child_view AS (
           |    ${convertToSQL(f.child)}
           |  ),
           |  dex_ppk_filter AS (
           |    SELECT child_view.*, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter FROM child_view WHERE $labelColExpr = ${nextLabel(labelPrfKeyExpr, s"${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}")}
           |    UNION ALL
           |    SELECT child_view.*, dex_ppk_filter.counter + 1 AS counter FROM child_view, dex_ppk_filter WHERE child_view.$labelColExpr = ${nextLabel(labelPrfKeyExpr, "dex_ppk_filter.counter + 1")}
           |  )
           |  SELECT $outputCols FROM dex_ppk_filter
           |)
         """.stripMargin*/
        /*s"""
           |(
           |  WITH RECURSIVE child_view AS (
           |    ${convertToSQL(f.child)}
           |  ),
           |  dex_ppk_filter AS (
           |    SELECT child_view.*, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter, 1 AS gap FROM child_view WHERE $labelColExpr = ${nextLabel(labelPrfKeyExpr, s"${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}")}
           |    UNION ALL
           |    SELECT child_view.*, dex_ppk_filter.counter + gap AS counter, 2 * gap AS gap FROM child_view, dex_ppk_filter WHERE child_view.$labelColExpr = ${nextLabel(labelPrfKeyExpr, "dex_ppk_filter.counter + gap")}
           |  )
           |  SELECT $outputCols FROM dex_ppk_filter
           |)
         """.stripMargin*/
        s"""
           |  WITH RECURSIVE dex_ppk_filter($ridOrder, counter) AS (
           |    SELECT ${f.filterTableName}.rid, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter FROM ${f.filterTableName} WHERE $labelColExpr = $firstLabelExpr
           |    UNION ALL
           |    SELECT ${f.filterTableName}.rid, dex_ppk_filter.counter + 1 AS counter FROM ${f.filterTableName}, dex_ppk_filter WHERE ${f.filterTableName}.$labelColExpr = $nextLabelExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_filter
         """.stripMargin
      case f: DexPseudoPrimaryKeyFilter =>
        val labelPrfKeyExpr = Literal(dexMasterTrapdoorForPred(f.predicate, None))
        val labelColExpr = dialect.quoteIdentifier(f.labelColumn)
        val outputCols = f.output.map(_.dialectSql(dialect)).mkString(", ")
        val firstLabelExpr = catalystEmmLabelExprOf(
          labelPrfKeyExpr, DexConstants.cashCounterStart
        ).dialectSql(dialect)
        val nextLabelExpr = catalystEmmLabelExprOf(labelPrfKeyExpr, Add($"dex_ppk_filter.counter", 1)).dialectSql(dialect)

        s"""
           |  WITH RECURSIVE dex_ppk_filter($outputCols, counter) AS (
           |    SELECT ${f.filterTableName}.*, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter FROM ${f.filterTableName} WHERE $labelColExpr = $firstLabelExpr
           |    UNION ALL
           |    SELECT ${f.filterTableName}.*, dex_ppk_filter.counter + 1 AS counter FROM ${f.filterTableName}, dex_ppk_filter WHERE ${f.filterTableName}.$labelColExpr = $nextLabelExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_filter
         """.stripMargin

      case j: DexPkfkMaterializationAwareJoin =>
        val (leftChildView, rightChildView) = (convertToSQL(j.left), convertToSQL(j.right))
        val labelColExpr = j.labelColumnOrder.dialectSql(dialect)
        val outputCols = j.output.map(_.dialectSql(dialect)).mkString(", ")
        val masterTrapdoor = dexMasterTrapdoorForPred(j.predicate, None)
        def secondaryTrapdoorExpr(ridCol: DialectSQLTranslatable) =
          catalystTrapdoorExprOf(Literal(masterTrapdoor), ridCol)
        val leftChildViewOutputCols = j.left.outputSet.map(_.dialectSql(dialect)).mkString(", ")
        val rightChildViewOutputCols = j.right.outputSet.map(_.dialectSql(dialect)).map(x => s"right_child_view.$x").mkString(", ")
        s"""
           |  WITH RECURSIVE left_child_view AS ${j.leftMs.sql} (
           |    $leftChildView
           |  ),
           |  right_child_view AS ${j.rightMs.sql} (
           |    $rightChildView
           |  ),
           |  dex_ppk_join AS (
           |    SELECT $leftChildViewOutputCols, $rightChildViewOutputCols, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM left_child_view, right_child_view
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr(j.leftTableRid), DexConstants.cashCounterStart).dialectSql(dialect)} = right_child_view.$labelColExpr
           |
           |    UNION ALL
           |
           |    SELECT $leftChildViewOutputCols, $rightChildViewOutputCols, counter + 1 AS counter
           |    FROM dex_ppk_join, right_child_view
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr(j.leftTableRid), Add($"counter", 1)).dialectSql(dialect)} = right_child_view.$labelColExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_join
         """.stripMargin

      case j: DexPkfkDependentJoin =>
        val (leftChildView, rightChildView) = (convertToSQL(j.left), convertToSQL(j.right))
        val leftChildViewOutputCols = j.left.outputSet.map(_.dialectSql(dialect)).mkString(", ")
        val labelColExpr = j.right.output.find(_.name.contains(j.labelColumn)).get.dialectSql(dialect)
        val outputCols = j.output.map(_.dialectSql(dialect)).mkString(", ")
        val (leftRidExpr, rightRidExpr) = (j.leftTableRid.dialectSql(dialect), j.rightTableRid.dialectSql(dialect))
        // val joinPredPrefixExpr = dialect.compileValue(j.predicate).asInstanceOf[String]
        // val masterTrapdoor = dexTrapdoor(masterSecret.hmacKey.getEncoded, joinPredPrefixExpr)
        val masterTrapdoor = dexMasterTrapdoorForPred(j.predicate, None)
        def secondaryTrapdoorExpr(ridCol: DialectSQLTranslatable) =
          catalystTrapdoorExprOf(Literal(masterTrapdoor), ridCol) //.dialectSql(dialect)

        // first generate labelPrfKeys for each primary key
        // Question 1: join left child view and right child view each recursion or join right child view after all recursions?
        // Question 2: join left child view or join from left table? Left child view might have been joined with other tables already
        /*s"""
           |(
           |  WITH RECURSIVE left_child_view AS (
           |    ${convertToSQL(j.leftChildView)}
           |  ),
           |  right_child_view AS (
           |    ${convertToSQL(j.rightChildView)}
           |  ),
           |  dex_ppk_join AS (
           |    SELECT left_child_view.*, right_child_view.*, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM left_child_view, right_child_view
           |    WHERE ${nextLabel(emmKeyColOfPrimaryKeyJoin(prfKey, j.leftChildViewRid, j.predicate), s"${Literal(DexConstants.cashCounterStart).dialectSql(dialect)}")} = right_child_view.$labelColExpr
           |
           |    UNION ALL
           |
           |    SELECT $leftChildViewOutputCols, right_child_view.*, counter + 1 AS counter
           |    FROM dex_ppk_join, right_child_view
           |    WHERE ${nextLabel(emmKeyColOfPrimaryKeyJoin(prfKey, j.leftChildViewRid, j.predicate), "counter + 1")} = right_child_view.$labelColExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_join
           |)
         """.stripMargin*/
        s"""
           |  WITH RECURSIVE left_child_view AS (
           |    $leftChildView
           |  ),
           |  right_child_view AS (
           |    $rightChildView
           |  ),
           |  dex_ppk_join AS (
           |    SELECT $leftChildViewOutputCols, right_child_view.*, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM left_child_view, right_child_view
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr(j.leftTableRid), DexConstants.cashCounterStart).dialectSql(dialect)} = right_child_view.$labelColExpr
           |
           |    UNION ALL
           |
           |    SELECT $leftChildViewOutputCols, right_child_view.*, counter + 1 AS counter
           |    FROM dex_ppk_join, right_child_view
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr(j.leftTableRid), Add($"counter", 1)).dialectSql(dialect)} = right_child_view.$labelColExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_join
         """.stripMargin
      case j: DexPkfkIndepJoin =>
        val labelColExpr = dialect.quoteIdentifier(j.labelColumn)
        val outputCols = j.output.map(_.dialectSql(dialect)).mkString(", ")
        val (leftRidExpr, rightRidExpr) = (j.leftTableRid.dialectSql(dialect), j.rightTableRid.dialectSql(dialect))
        // val joinPredPrefixExpr = dialect.compileValue(j.predicate).asInstanceOf[String]
        // val masterTrapdoor = dexTrapdoor(masterSecret.hmacKey.getEncoded, joinPredPrefixExpr)
        val masterTrapdoor = dexMasterTrapdoorForPred(j.predicate, None)
        def secondaryTrapdoorExpr(ridCol: DialectSQLTranslatable) =
          catalystTrapdoorExprOf(Literal(masterTrapdoor), ridCol) //.dialectSql(dialect)

        s"""
           |(
           |  WITH RECURSIVE dex_ppk_join($leftRidExpr, $rightRidExpr, counter) AS (
           |    SELECT ${j.leftTableName}.rid AS $leftRidExpr, ${j.rightTableName}.rid AS $rightRidExpr, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM ${j.leftTableName}, ${j.rightTableName}
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr($"${j.leftTableName}.rid"), DexConstants.cashCounterStart).dialectSql(dialect)} = $labelColExpr
           |
           |    UNION ALL
           |
           |    SELECT $leftRidExpr, ${j.rightTableName}.rid AS $rightRidExpr, counter + 1 AS counter
           |    FROM dex_ppk_join, ${j.rightTableName}
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr($"${j.leftTableRid.name}"), Add($"counter", 1)).dialectSql(dialect)} = $labelColExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_join
           |)
         """.stripMargin
      case j: DexPkfkRightTableJoin =>
        require(isTableScan(j.right))
        val labelColExpr = j.right.output.find(_.name.contains(j.labelColumn)).get.dialectSql(dialect)
        val outputCols = j.output.map(_.dialectSql(dialect)).mkString(", ")
        val (leftRidExpr, rightRidExpr) = (j.leftTableRid.dialectSql(dialect), j.rightTableRid.dialectSql(dialect))
        // val joinPredPrefixExpr = dialect.compileValue(j.predicate).asInstanceOf[String]
        // val masterTrapdoor = dexTrapdoor(masterSecret.hmacKey.getEncoded, joinPredPrefixExpr)
        val masterTrapdoor = dexMasterTrapdoorForPred(j.predicate, None)
        def secondaryTrapdoorExpr(ridCol: DialectSQLTranslatable) =
          catalystTrapdoorExprOf(Literal(masterTrapdoor), ridCol) //.dialectSql(dialect)

        s"""
           |(
           |  WITH RECURSIVE right_child_view AS (
           |    ${convertToSQL(j.right)}
           |  ),
           |  dex_ppk_join AS (
           |    SELECT ${j.leftTableName}.rid AS $leftRidExpr, right_child_view.*, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM ${j.leftTableName}, right_child_view
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr($"${j.leftTableName}.rid"), DexConstants.cashCounterStart).dialectSql(dialect)} = right_child_view.$labelColExpr
           |
           |    UNION ALL
           |
           |    SELECT $leftRidExpr, right_child_view.*, counter + 1 AS counter
           |    FROM dex_ppk_join, right_child_view
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr($"${j.leftTableRid.name}"), Add($"counter", 1)).dialectSql(dialect)} = right_child_view.$labelColExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_join
           |)
         """.stripMargin
      case j: DexPkfkLeftDependentJoin =>
        // val labelColExpr = j.right.output.find(_.name.contains(j.labelColumn)).get.dialectSql(dialect)
        val labelColExpr = j.labelColumn
        val outputCols = j.output.map(_.dialectSql(dialect)).mkString(", ")
        val (leftRidExpr, rightRidExpr) = (j.leftTableRid.dialectSql(dialect), j.rightTableRid.dialectSql(dialect))
        val leftChildViewOutputCols = j.left.outputSet.map(_.dialectSql(dialect)).mkString(", ")
        val masterTrapdoor = dexMasterTrapdoorForPred(j.predicate, None)
        def secondaryTrapdoorExpr(ridCol: DialectSQLTranslatable) =
          catalystTrapdoorExprOf(Literal(masterTrapdoor), ridCol) //.dialectSql(dialect)

        s"""
           |(
           |  WITH RECURSIVE left_child_view AS NOT MATERIALIZED (
           |    ${convertToSQL(j.left)}
           |  ),
           |  dex_ppk_join AS (
           |    SELECT left_child_view.*, ${j.rightTableName}.rid AS $rightRidExpr, ${Literal(DexConstants.cashCounterStart).dialectSql(dialect)} AS counter
           |    FROM left_child_view, ${j.rightTableName}
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr(j.leftTableRid), DexConstants.cashCounterStart).dialectSql(dialect)} = $labelColExpr
           |
           |    UNION ALL
           |
           |    SELECT $leftChildViewOutputCols, ${j.rightTableName}.rid AS $rightRidExpr, counter + 1 AS counter
           |    FROM dex_ppk_join, ${j.rightTableName}
           |    WHERE ${catalystEmmLabelExprOf(secondaryTrapdoorExpr(j.leftTableRid), Add($"counter", 1)).dialectSql(dialect)} = $labelColExpr
           |  )
           |  SELECT $outputCols FROM dex_ppk_join
           |)
         """.stripMargin

      case SubqueryAlias(name, child) =>
        s"""
           |${convertToSQL(child)} AS ${name.unquotedString}
           |""".stripMargin

      case x => throw DexException("unsupported: " + x.getClass.toString)
    }

    private def isNaturalJoin(join: Join) = {
      join.condition.exists(isConjunctionOnly) &&
        join.condition.exists(hasEquiJoinConditionOnly) &&
        join.condition.collectFirst {
          case eq @ EqualTo(left: Attribute, right: Attribute) if left.name != right.name => eq
        }.isEmpty
    }

    private def isConjunctionOnly(condition: Expression) = {
      condition.find(x => x.isInstanceOf[Or]).isEmpty
    }

    private def hasEquiJoinConditionOnly(condition: Expression) = {
      condition.collectFirst {
        case bc: BinaryComparison if !bc.isInstanceOf[EqualTo] => bc
        case eq: EqualTo if !(eq.left.isInstanceOf[Attribute] && eq.right.isInstanceOf[Attribute]) => eq
      }.isEmpty
    }

    private def ridOrdersFromJoin(j: Join): (String, String) = {
      val lefts = j.condition.get.collect {
        case EqualTo(left, right) => left.asInstanceOf[DialectSQLTranslatable]
      }
      val rights = j.condition.get.collect {
        case EqualTo(left, right) => right.asInstanceOf[DialectSQLTranslatable]
      }
      (lefts.map(_.dialectSql(dialect)).mkString(", "), rights.map(_.dialectSql(dialect)).mkString(", "))
    }

    /*private def emmKeysFromDomainValueColumn(prfKey: String, domainValueCol: String, predicate: String): (String, String) = {
      // todo: append 1 and 2 to form two keys
      // todo: randomized the predicate using prfKey
      val prfKeyCol = s"'$predicate' || '~' || $domainValueCol"
      val decKeyCol = prfKeyCol
      (prfKeyCol, decKeyCol)
    }

    private def emmKeysOfRidCol(prfKey: String, ridCol: String, predicate: String): (String, String) = {
      // todo: append 1 and 2 to form two keys
      // todo: randomized the predicate using prfKey
      val predRidCol = s"'$predicate' || '~' || $ridCol"
      (predRidCol, predRidCol)
    }

    private def emmKeys(predicate: String): (String, String) =
      // todo: append 1 and 2 to form two keys
      (dialect.compileValue(predicate).asInstanceOf[String],
        dialect.compileValue(predicate).asInstanceOf[String])

    private def emmKeyColOfPrimaryKeyJoin(prfKey: String, primaryKeyCol: String, predicate: String): String = {
      //Concat(Seq(predicate, "~", primaryKeyCol)).dialectSql(dialect.quoteIdentifier)
      s"'$predicate' || '~' || $primaryKeyCol"
    }

    private def emmKeyForPrimaryKeyFilter(predicate: String): String =
      dialect.compileValue(predicate).asInstanceOf[String]

    private def nextLabel(labelPrfKey: String, nextCounter: String): String =
      s"$labelPrfKey || ${dialect.compileValue("~")} || $nextCounter"*/


    def generateSubqueryName() =
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
          val translator = DexPlanTranslator.ofPlan(p)
          log.warn("== To be translated == \n" + plan.treeString(verbose = true))
          translator.translate
      }
    }
  }

  object DexPlanTranslator {
    def ofPlan(dexPlan: DexPlan): DexPlanTranslator = dexPlan match {
      case p: SpxPlan =>
        log.warn("dexPlan=SpxPlan")
        SpxTranslator(p, p.compoundKeys)
      case p: DexCorrelationPlan =>
        log.warn("dexPlan=DexCorrelationPlan")
        DexCorrelationTranslator(p, p.compoundKeys)
      case p: DexDomainPlan =>
        log.warn("dexPlan=DexDomainPlan")
        DexDomainTranslator(p)
      case p: DexPkFkPlan =>
        log.warn("dexPlan=DexPkFkPlan")
        DexPkFkTranslator(p, p.primaryKeys, p.foreignKeys)
    }
  }

  sealed trait DexPlanTranslator {

    def dexPlan: DexPlan

    protected lazy val joinOrder: LogicalRelation => Int =
      dexPlan.collect {
        case l: LogicalRelation => l
      }.indexOf

    protected lazy val exprIdToTable: ExprId => LogicalRelation =
      dexPlan.collect {
        case l: LogicalRelation =>
          l.output.map(x => (x.exprId, l))
      }.flatten.toMap

    protected lazy val output = dexPlan.output.map(translateAttribute)

    def translate: LogicalPlan = {
      // We want to preserve the DexPlan nodes during the translation process because we will use them to determine
      // boundaries for non-Dex and Dex queries for SQL conversion later.
      // But we need to be careful when preserving the DexPlan due to recursive transformation of the tree.
      // There would be trouble if we did 'val newPlan = tranlsatePlan(dexPlan, None)' i.e. let the translatePlan()
      // to add back the DexPlan, because translatePlan() is recursive and it would have a hard time to differentiate
      // the root DexPlan to preserve and any subtree DexPlan (already translated, hence to ignore).
      // So we have to add back the root DexPlan here to avoid the ambiguity.
      val newChild = translatePlan(dexPlan.child)
      val newPlan = dexPlan match {
        case p: SpxPlan =>
          SpxPlan(newChild, p.compoundKeys)
        case p: DexCorrelationPlan =>
          DexCorrelationPlan(newChild, p.compoundKeys)
        case p: DexDomainPlan =>
          DexDomainPlan(newChild)
        case p: DexPkFkPlan =>
          DexPkFkPlan(newChild, p.primaryKeys, p.foreignKeys)
        case x => throw DexException("unsupported: " + x.getClass.getName)
      }
      newPlan.select(output: _*)
    }

    private def translateAttribute(attr: Attribute): NamedExpression = {
      DexDecode(
        catalystDecryptAttribute(dexColOrderOf(attr)),
        attr.dataType.asInstanceOf[AtomicType]
      ).as(attr.name)
    }

    private def dexColOrderOf(attr: Attribute): Attribute = $"${DexPrimitives.dexColNameOf(attr.name)}_${joinOrder(exprIdToTable(attr.exprId))}"

    protected def translatePlan(plan: LogicalPlan): LogicalPlan = {
      plan match {
        case d: DexPlan =>
          // Because we're transforming up the tree, we may encounter subtree DexPlan that has already been translated.
          // So here we simply return it as is
          d
        case l: LogicalRelation =>
          tableEncWithRidOrderOf(l)

        case p: Project =>
          // todo: projection push down
          translatePlan(p.child)

        case f: Filter =>
          val source = translatePlan(f.child)
          translateFormula(FilterFormula, f.condition, Seq(source), isNegated = false)

        case j: Join if j.joinType == Cross =>
          translatePlan(j.left).join(translatePlan(j.right), Cross)

        case j: Join if j.condition.isDefined =>
          val joinAttrs = nonIsNotNullPredsIn(Seq(j.condition.get))
          val leftAttrs = nonIsNotNullPredsIn(j.left.expressions)
          val rightAttrs = nonIsNotNullPredsIn(j.right.expressions)

          if (joinAttrs.equals(leftAttrs ++ rightAttrs)) {
            // join completely coincides with filters
            // e.g. T1(a, b) join T2(c, d) on a = c and b = d where a = c = 1 and b = d = 2
            // note: this cross join only works for equality filter and joins
            val leftView = translatePlan(j.left)
            val rightView = translatePlan(j.right)
            leftView.join(rightView, Cross)
          } else {
            // e.g.  T1(a, b) join T2(c, d) on a = c and b = d where a = c = 1
            val leftView = translatePlan(j.left)
            val joinView = translateFormula(JoinFormula, j.condition.get, Seq(leftView), isNegated = false)
            val rightView = translatePlan(j.right)
            j.joinType match {
              case Inner =>
                joinView.join(rightView, NaturalJoin(Inner))
              case LeftSemi =>
                /*if (j.right.isInstanceOf[LogicalRelation]) {
                  // rightview is useless
                  leftView.join(
                    joinView,
                    NaturalJoin(LeftSemi))
                } else {*/
                  leftView.join(
                    joinView.join(rightView, NaturalJoin(LeftSemi)),
                    NaturalJoin(LeftSemi)) // less efficient than RightSemi by having more joins on leftviews
                //}
              case RightSemi =>
                //joinView.join(rightView, NaturalJoin(RightSemi))
                rightView.join(joinView, NaturalJoin(LeftSemi))
              case LeftAnti =>
                /*if (j.right.isInstanceOf[LogicalRelation]) {
                  leftView.join(
                    joinView,
                    NaturalJoin(LeftAnti)) // less efficient than RightSemi by having more joins on leftviews
                } else {*/
                  leftView.join(
                    joinView.join(rightView, NaturalJoin(LeftSemi)),
                    NaturalJoin(LeftAnti)) // less efficient than RightSemi by having more joins on leftviews
                //}

              case RightAnti =>
                rightView.join(joinView, NaturalJoin(LeftAnti))
              case RightOuter =>
                joinView.join(rightView, NaturalJoin(RightOuter))
              case x => throw DexException("unsupported: " + x.getClass.getName)
            }

          }

        case x => throw DexException("unsupported: " + x.toString)
      }
    }

    protected def tableNameFromLogicalRelation(relation: LogicalRelation): String = relation.relation match {
      case j: JDBCRelation =>
        j.jdbcOptions.tableOrQuery
      case h: HadoopFsRelation =>
        // DataSource.resolveRelation(.)
        h.location match {
          case c: CatalogFileIndex =>
            c.table.identifier.table
          case i: InMemoryFileIndex =>
            val rootPathSet: Set[String] = i.partitionSpec() match {
              case p if p == PartitionSpec.emptySpec => i.rootPaths.map(_.getName).toSet
              case _ => i.rootPaths.map(_.getParent.getName).toSet
            }
            require(rootPathSet.size == 1)
            log.warn(s"InMemoryFileIndex=${rootPathSet.head}")
            rootPathSet.head
          case x => throw DexException("unsupported: " + x.getClass.toString)
        }
      case x => throw DexException("unsupported: " + x.getClass.toString)
    }

    private def nonIsNotNullPredsIn(conds: Seq[Expression]): AttributeSet = {
      conds.flatMap(_.collect {
        case x: BinaryComparison => x.references
      }).reduceOption(_ ++ _).getOrElse(AttributeSet.empty)
    }

    protected def translateFormula(formulaType: FormulaType, condition: Expression, childViews: Seq[LogicalPlan], isNegated: Boolean): LogicalPlan = {
      condition match {
        case p: EqualTo => formulaType match {
          case FilterFormula =>
            require(childViews.size == 1)
            translateFilterPredicate(p, childViews.headOption.get, isNegated)
          case JoinFormula =>
            translateJoinPredicate(p, childViews)
        }

        case And(left, right) if !isNegated =>
          translateFormula(formulaType, right, Seq(translateFormula(formulaType, left, childViews, isNegated)), isNegated)

        case Or(left, right) if !isNegated =>
          val lt = translateFormula(formulaType, left, childViews, isNegated)
          val rt = translateFormula(formulaType, right, childViews, isNegated)
          //rt.flatMap(r => lt.map(l => l unionDistinct r)).orElse(lt)
          lt unionDistinct rt

        case IsNotNull(attr: Attribute) =>
          require(childViews.size == 1)
          childViews.headOption.get

        case In(attr: Attribute, list: Seq[Expression]) if formulaType == FilterFormula =>
          val pred = if (isNegated) {
            Not(list.map(expr => EqualTo(attr, expr)).reduce[Predicate]((p1, p2) => And(p1, p2)))
          } else {
            list.map(expr => EqualTo(attr, expr)).reduce[Predicate]((p1, p2) => Or(p1, p2))
          }
          translateFormula(formulaType, pred, childViews, isNegated = false)

        case Not(p: Predicate) if formulaType == FilterFormula =>
          translateFormula(formulaType, p, childViews, isNegated = true)

        case x => throw DexException("unsupported: " + x.toString)
      }
    }

    @scala.annotation.tailrec
    private def translateFilterPredicate(p: Predicate, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = p match {
      case EqualTo(left: Attribute, right@Literal(value, dataType)) =>
        val colName = left.name
        val tableRel = exprIdToTable(left.exprId)
        val ridOrder = s"rid_${joinOrder(tableRel)}"
/*        val valueStr = dataType match {
          case IntegerType => s"${value.asInstanceOf[Int]}"
          case StringType => s"$value"
          case x => throw DexException("unsupported: " + x.toString)
        }*/
        dexFilterOf(tableRel, colName, value, ridOrder, childView, isNegated)

      case EqualTo(left: Attribute, right: Attribute) if left.name < right.name =>
        val colNames = (left.name, right.name)
        val tableRels = (exprIdToTable(left.exprId), exprIdToTable(right.exprId))
        require(tableRels._1 == tableRels._2)
        val tableRel = tableRels._1
        val ridOrder = s"rid_${joinOrder(tableRel)}"
        // todo: extend this same value columns case to DexPkFk
        // assume TFilter has entry like col1~col2 for the _same_ table
        dexFilterOf(tableRel, colNames._1, colNames._2, ridOrder, childView, isNegated)

      case EqualTo(left: Attribute, right: Attribute) if left.name > right.name =>
        translateFilterPredicate(EqualTo(right, left), childView, isNegated)

      case x => throw DexException("unsupported: " + x.toString)
    }

    protected def dexFilterOf(predicateTable: LogicalRelation, predicateColname: String, predicateValue: Any, ridOrder: String, childView: LogicalPlan, isNegated: Boolean): LogicalPlan

    case class JoinAttrs(left: Attribute, right: Attribute) {
      val (leftColName, rightColName) = (left.name, right.name)
      val (leftTableRel, rightTableRel) = (exprIdToTable(left.exprId), exprIdToTable(right.exprId))
      val (leftTableName, rightTableName) = (tableNameFromLogicalRelation(leftTableRel), tableNameFromLogicalRelation(rightTableRel))
      val (leftTableOrder, rightTableOrder) = (joinOrder(leftTableRel), joinOrder(rightTableRel))
      val (leftRidOrder, rightRidOrder) = (s"rid_$leftTableOrder", s"rid_$rightTableOrder")
      val (leftQualifiedName, rightQualifiedName) = (s"$leftTableName.$leftColName", s"$rightTableName.$rightColName")
      val (leftTableAttr, rightTableAttr) = (TableAttributeAtom(leftTableName, leftColName), TableAttributeAtom(rightTableName, rightColName))

      def ridOrderAttrsGiven(childView: LogicalPlan): (Option[Attribute], Option[Attribute]) =
        (childView.output.find(_.name == leftRidOrder), childView.output.find(_.name == rightRidOrder))

      def orderedAlphabetically(): JoinAttrs = if (leftQualifiedName <= rightQualifiedName) {
        this
      } else {
        JoinAttrs(right, left)
      }
    }

    private def translateJoinPredicate(p: Predicate, childViews: Seq[LogicalPlan]): LogicalPlan = p match {
      case EqualTo(left: Attribute, right: Attribute) =>
        translateEquiJoin(JoinAttrs(left, right), childViews)
      case x => throw DexException("unsupported: " + x.getClass.toString)
    }

    protected def translateEquiJoin(joinAttrs: JoinAttrs, chidlViews: Seq[LogicalPlan]): LogicalPlan

    protected def tableEncNameOf(tableName: String): String = dexTableNameOf(tableName)

    protected def tableEncWithRidOrderOf(tablePlain: LogicalRelation): LogicalPlan = {
      val tableEnc = tableEncOf(tablePlain)
      renameRidWithJoinOrder(tableEnc, tablePlain)
    }

    protected def tableEncOf(tablePlain: LogicalRelation): LogicalRelation = {
      LogicalRelation(
        DataSource.apply(
          sparkSession,
          className = "jdbc",
          options = Map(
            JDBCOptions.JDBC_URL -> SQLConf.get.dexEncryptedDataSourceUrl,
            JDBCOptions.JDBC_TABLE_NAME -> tableEncNameOf(tableNameFromLogicalRelation(tablePlain)))).resolveRelation())
    }

    def renameRidWithJoinOrder(tableEnc: LogicalPlan, tablePlain: LogicalRelation): LogicalPlan = {
      val output = tableEnc.output
      val order = joinOrder(tablePlain)
      val columns = output.map { col =>
        if (resolver(col.name, ridCol)) {
          Column(col).as(s"${ridCol}_$order").expr
        } else {
          Column(col).as(s"${col.name}_$order").expr
        }
      }
      tableEnc.select(columns: _*)
    }
  }

  sealed trait CompoundKeyAwareTranslator extends DexPlanTranslator {
    def compoundKeys: Set[String] // can contain atomic key

    private def isCompoundKeyJoin(condition: Expression): Boolean = {
      val attrLefts = condition.collect {
        case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrLeft
      }
      val attrRights = condition.collect {
        case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrRight
      }
      attrLefts.size > 1 && attrRights.size > 1 && {
        val attrCompoundLeft = TableAttributeCompound("dummy", attrLefts.map(_.name)).attr
        val attrCompoundRight = TableAttributeCompound("dummy", attrRights.map(_.name)).attr
        compoundKeys.contains(attrCompoundLeft) && compoundKeys.contains(attrCompoundRight)
      }
    }

    override protected def translatePlan(plan: LogicalPlan): LogicalPlan = plan match {
        // todo: case where compound key is only one of the conjunctive predicates in this join
      case j: Join if j.condition.isDefined && isCompoundKeyJoin(j.condition.get) =>
        val joinCompoundCond = {
          val attrLefts = j.condition.get.collect {
            case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrLeft
          }
          val attrRights = j.condition.get.collect {
            case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrRight
          }
          require(attrLefts.map(_.exprId).map(exprIdToTable).toSet.size == 1, "compound to same table")
          require(attrRights.map(_.exprId).map(exprIdToTable).toSet.size == 1, "compound to same table")
          val attrCompoundLeft = TableAttributeCompound("dummy", attrLefts.map(_.name)).attr
          val attrCompoundRight = TableAttributeCompound("dummy", attrRights.map(_.name)).attr
          // Hack: reuse the expr id for one of attrLefts and one of attrRights so that exprIdToTable works later
          EqualTo(attrLefts.head.withName(attrCompoundLeft), attrRights.head.withName(attrCompoundRight))
        }
        val compoundJoinPlan = j.copy(condition = Some(joinCompoundCond))
        super.translatePlan(compoundJoinPlan)

      case _ => super.translatePlan(plan)
    }
  }

  sealed trait StandaloneTranslator extends DexPlanTranslator {
    override protected def dexFilterOf(predicateTable: LogicalRelation, predicateColName: String, predicateValue: Any, ridOrder: String, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = {
      val predicateTableName = tableNameFromLogicalRelation(predicateTable)
      val predicate = dexFilterPredicate(dexFilterPredicatePrefixOf(predicateTableName, predicateColName))(predicateValue)

      if (isTableScan(childView)) {
        val ridFilter = DexRidFilter(predicate, tFilter)
          .select(DexDecrypt($"value_dec_key", $"value").as(ridOrder))
        if (isNegated) {
          childView.join(ridFilter, UsingJoin(LeftAnti, Seq(ridOrder)))
        } else {
          require(childView.output.exists(_.name == ridOrder))
          childView.join(ridFilter, UsingJoin(LeftSemi, Seq(ridOrder)))
        }
      } else {
        val masterTrapdoor = dexMasterTrapdoorForPred(predicate, None)
        val joinType = if (isNegated) LeftAnti else LeftSemi
        childView.join(tDepFilter, joinType, Some(catalystTrapdoorExprOf(masterTrapdoor, $"$ridOrder") === $"${DexConstants.tDepFilterCol}"))
      }
    }
  }

  case class SpxTranslator(dexPlan: DexPlan, compoundKeys: Set[String]) extends StandaloneTranslator with CompoundKeyAwareTranslator {
    override protected def translateEquiJoin(joinAttrsUnordered: JoinAttrs, childViews: Seq[LogicalPlan]): LogicalPlan = {
      require(childViews.size == 1)
      val childView = childViews.headOption.get
      val joinAttrs = joinAttrsUnordered.orderedAlphabetically()
      val predicate = s"${joinAttrs.leftTableName}~${joinAttrs.leftColName}~${joinAttrs.rightTableName}~${joinAttrs.rightColName}"
      val ridJoin = SpxRidUncorrelatedJoin(predicate, tUncorrelatedJoin)
          .select(DexDecrypt($"value_dec_key", $"value_left").as(joinAttrs.leftRidOrder), DexDecrypt($"value_dec_key", $"value_right").as(joinAttrs.rightRidOrder))

      childView.join(ridJoin, NaturalJoin(Inner))
    }
  }

  case class DexCorrelationTranslator(dexPlan: DexPlan, compoundKeys: Set[String]) extends StandaloneTranslator with CompoundKeyAwareTranslator {
    override protected def translateEquiJoin(joinAttrs: JoinAttrs, childViews: Seq[LogicalPlan]): LogicalPlan = {
      require(childViews.size == 1)
      val childView = childViews.headOption.get
      //val hasJoinOnChildView = childView.find(_.isInstanceOf[DexRidCorrelatedJoin]).nonEmpty
      val hasFilterOnChildView = childView.find(_.isInstanceOf[DexRidFilter]).nonEmpty

      def predicatePrefixOf(joinAttrs: JoinAttrs, reverse: Boolean): String = {
        if (reverse)
          //s"${joinAttrs.rightTableName}~${joinAttrs.rightColName}~${joinAttrs.leftTableName}~${joinAttrs.leftColName}"
          dexCorrJoinPredicatePrefixOf(joinAttrs.rightTableAttr, joinAttrs.leftTableAttr)
        else
          //s"${joinAttrs.leftTableName}~${joinAttrs.leftColName}~${joinAttrs.rightTableName}~${joinAttrs.rightColName}"
          dexCorrJoinPredicatePrefixOf(joinAttrs.leftTableAttr, joinAttrs.rightTableAttr)
      }

      def newRidJoinOf(l: Attribute, leftSubquery: LogicalPlan, rightRidOrder: String, predicate: String) = {
        // "right" relation is a new relation to join
        val ridJoin = DexRidCorrelatedJoin(predicate, leftSubquery, tCorrelatedJoin, l)
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
      }

      joinAttrs.ridOrderAttrsGiven(childView) match {
        case (Some(l), None) =>
          if (hasFilterOnChildView) {
            newRidJoinOf(l, childView, joinAttrs.rightRidOrder, predicatePrefixOf(joinAttrs, reverse = false))
          } else {
            val leftSubquery = tableEncOf(joinAttrs.leftTableRel).select(col("rid").as(joinAttrs.leftRidOrder).expr)
            val leftSubqueryRidOrder = leftSubquery.outputSet.find(_.name == joinAttrs.leftRidOrder).get
            childView.join(
              newRidJoinOf(leftSubqueryRidOrder, leftSubquery, joinAttrs.rightRidOrder, predicatePrefixOf(joinAttrs, reverse = false)),
              UsingJoin(Inner, Seq(joinAttrs.leftRidOrder))
            )
          }

        case (Some(l), Some(r)) if l == r =>
          // Self join: attr == attr
          val ridJoin = DexRidCorrelatedJoin(predicatePrefixOf(joinAttrs, reverse = false), childView, tCorrelatedJoin, l)
          val ridJoinProject = childView.output
          ridJoin.select(ridJoinProject: _*)

        case (Some(l), Some(r)) =>
          // "right" relation is a previously joined relation
          // e.g. (T1 join T2 on T1.c = T2.c) join T3 on (T1.a = T3.a and T2.b = T3.b) is seen as
          // ((T1 join T2 on ...) join T3_A on T1.a=T3.a) left-semijoin T3_B on T2.b=T3.b.  The last join T2.b=T3.b is this case
          // the last left-semijoin is equivalent to inner join T3_B on T2.b=T3_B.b and T3_B.a=T3_A.a
          // don't have extra "value_dec_key" column
          val ridJoin = DexRidCorrelatedJoin(predicatePrefixOf(joinAttrs, reverse = false), childView, tCorrelatedJoin, l).where(EqualTo(r, DexDecrypt($"value_dec_key", $"value")))
          val ridJoinProject = childView.output
          ridJoin.select(ridJoinProject: _*)
          // Another approach

          if (hasFilterOnChildView) {
            val ridJoin = DexRidCorrelatedJoin(predicatePrefixOf(joinAttrs, reverse = false), childView, tCorrelatedJoin, l).where(EqualTo(r, DexDecrypt($"value_dec_key", $"value")))
            val ridJoinProject = ridJoin.output.collect {
              case x: Attribute if x.name != "value_dec_key" && x.name != "value" => // remove extra value_dec_key column
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
          } else {
            val leftSubquery = tableEncOf(joinAttrs.leftTableRel).select(col("rid").as(joinAttrs.leftRidOrder).expr)
            val leftSubqueryRidOrder = leftSubquery.outputSet.find(_.name == joinAttrs.leftRidOrder).get
            childView.join(
              newRidJoinOf(leftSubqueryRidOrder, leftSubquery, joinAttrs.rightRidOrder, predicatePrefixOf(joinAttrs, reverse = false)),
              UsingJoin(LeftSemi, Seq(joinAttrs.leftRidOrder, joinAttrs.rightRidOrder))
            )
          }

        case (None, Some(r)) =>
          // "left" relation is a new relation to join
          if (hasFilterOnChildView) {
            newRidJoinOf(r, childView, joinAttrs.leftRidOrder, predicatePrefixOf(joinAttrs, reverse = true))
          } else {
            val rightSubquery = tableEncOf(joinAttrs.rightTableRel).select(col("rid").as(joinAttrs.rightRidOrder).expr)
            val rightSubqueryRidOrder = rightSubquery.outputSet.find(_.name == joinAttrs.rightRidOrder).get
            newRidJoinOf(rightSubqueryRidOrder, rightSubquery, joinAttrs.leftRidOrder, predicatePrefixOf(joinAttrs, reverse = true))
              .join(childView, UsingJoin(Inner, Seq(joinAttrs.rightRidOrder)))
          }


        case x => throw DexException("unsupported: (None, None)")
      }
    }
  }

  case class DexDomainTranslator(dexPlan: DexPlan) extends StandaloneTranslator {
    override protected def translateEquiJoin(joinAttrs: JoinAttrs, childViews: Seq[LogicalPlan]): LogicalPlan = {
      require(childViews.size == 1)
      val childView = childViews.headOption.get
      val (leftPredicate, rightPredicate) = (s"${joinAttrs.leftTableName}~${joinAttrs.leftColName}",s"${joinAttrs.rightTableName}~${joinAttrs.rightColName}")
      val (leftDomainValues, rightDomainValues) = (
        DexDomainValues(leftPredicate, tDomain).select(DexDecrypt($"value_dec_key", $"value").as(s"value_dom")),
        DexDomainValues(rightPredicate, tDomain).select(DexDecrypt($"value_dec_key", $"value").as(s"value_dom"))
      )
      val intersectDomain = leftDomainValues.intersect(rightDomainValues, isAll = false)
      /*val (leftDomainRids, rightDomainRids) = (
        DexDomainRids(leftPredicate, intersectDomain, tFilter, $"value_dom").select("value_dom", DexDecrypt($"value_dec_key", $"value").as(joinAttrs.leftRidOrder)),
        DexDomainRids(rightPredicate, intersectDomain, tFilter, $"value_dom").select("value_dom", DexDecrypt($"value_dec_key", $"value").as(joinAttrs.rightRidOrder))
      )
      leftDomainRids.join(rightDomainRids, NaturalJoin(Inner)).join(childView, NaturalJoin(Inner))*/
      // Intersetingly, the above won't be optimized because intersectDomain appears in two places, each in a CTE.
      // CTEs are "optimization barriers" so they can't be extracted. (Ideally only one run is enough)
      // On the other hand, cannot change the join order to the appearinlyg suboptimal (left join domain) inner join (right join domain)
      // where inner join serves as intersection.  Can't hope the optimizer makes the right decisino to change it to
      // (left join) domain intersect domain (right join), because usually this re-write is incorrect.
      DexDomainJoin(leftPredicate, rightPredicate, intersectDomain, tFilter, tFilter)
        .select(
          DexDecrypt($"value_dec_key_left", $"value_left").as(joinAttrs.leftRidOrder),
          DexDecrypt($"value_dec_key_right", $"value_right").as(joinAttrs.rightRidOrder)
        ).join(childView, NaturalJoin(Inner))
    }
  }

  case class DexPkFkTranslator(dexPlan: DexPlan, primaryKeys: Set[String], foreignKeys: Set[String]) extends DexPlanTranslator {
    require(primaryKeys.nonEmpty && foreignKeys.nonEmpty)

    override protected def translatePlan(plan: LogicalPlan): LogicalPlan = plan match {
      case l: LogicalRelation =>
        // Assume the filter operator will output childView schema
        // Assume the join operator will output childView schema
        tableEncWithRidOrderOf(l)

        // fixme: case when join is conjunction, only subset of the predicates are compound keys
      case j: Join if j.condition.isDefined && isCompoundKeyJoin(j.condition.get) =>
        val joinCompoundCond = {
          val attrLefts = j.condition.get.collect {
            case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrLeft
          }
          val attrRights = j.condition.get.collect {
            case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrRight
          }
          require(attrLefts.map(_.exprId).map(exprIdToTable).toSet.size == 1, "compound to same table")
          require(attrRights.map(_.exprId).map(exprIdToTable).toSet.size == 1, "compound to same table")
          val attrCompoundLeft = TableAttributeCompound("dummy", attrLefts.map(_.name)).attr
          val attrCompoundRight = TableAttributeCompound("dummy", attrRights.map(_.name)).attr
          // Hack: reuse the expr id for one of attrLefts and one of attrRights so that exprIdToTable works later
          EqualTo(attrLefts.head.withName(attrCompoundLeft), attrRights.head.withName(attrCompoundRight))
        }
        val compoundJoinPlan = j.copy(condition = Some(joinCompoundCond))
        translatePlan(compoundJoinPlan)

      // this case is handled in translanteEquiJoin
      /*case j: Join if j.condition.isDefined && hasPkFkJoinWithFkFilter(j) =>
        // PK join Filter(FK) has a subtle issue if execute in post-order:
        // For pk-rows with pk-fk-pibas-counter having any prefix sequence not satisify their fk-rows' filter,
        // then pk-rows with ALL sequence would not be included in join (due to counter increment).
        // E.g.
        // supplier join partsupp filtered on ps_comment = psb
        // -------------------
        // supp_key | ps_comment
        // ----------------------
        // pk10~0   | psa
        // pk10~1   | psa
        // pk10~2   | psb
        // -------------------
        // Because first row doesn't match filter, pibas-counter 0,1 would not be included, so by induction 2 would
        // not be included in the join too.  But 2 should be in the join.
        // The fix is just to reverse this type of join and filter to fk-pk join with filter on fk.
        val reverseJoinPlan = j.copy(condition = Some(reverseJoinCondition(j.condition.get)))
        translatePlan(reverseJoinPlan)*/

      case j: Join if j.condition.isDefined =>
        translateJoinView(j.left, j.right, j.condition.get)

      case _ => super.translatePlan(plan)
    }

    private def isCompoundKeyJoin(condition: Expression): Boolean = {
      // order sensitive components in compound key (A, B) need to join in order A = ... AND B = ...
      val attrLefts = condition.collect {
        case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrLeft
      }
      val attrRights = condition.collect {
        case EqualTo(attrLeft: Attribute, attrRight: Attribute) => attrRight
      }

      attrLefts.size > 1 && attrRights.size > 1 && {
        val attrCompoundLeft = TableAttributeCompound("dummy", attrLefts.map(_.name)).attr
        val attrCompoundRight = TableAttributeCompound("dummy", attrRights.map(_.name)).attr
        (primaryKeys.contains(attrCompoundLeft) && foreignKeys.contains(attrCompoundRight)) ||
          (primaryKeys.contains(attrCompoundRight) && foreignKeys.contains(attrCompoundLeft))
      }


    }

    private def translateJoinView(left: LogicalPlan, right: LogicalPlan, joinCond: Expression) = {
      val leftView = translatePlan(left)
      val rightView = translatePlan(right)
      val joinView = translateFormula(JoinFormula, joinCond, Seq(leftView, rightView), isNegated = false)
      joinView
    }

    private def reverseJoinCondition(expr: Expression): Expression = expr match {
      case EqualTo(left: Attribute, right: Attribute) => EqualTo(right, left)
      case _ => throw DexException("unsupported")
    }

    private def hasPkFkJoinWithFkFilter(j: Join): Boolean = {
      // hasPkFkJoin(j.condition.get) && j.right.isInstanceOf[Filter] && isFkFilter(j.right.asInstanceOf[Filter])
      fksInAnyPkFkJoin(j.condition.get).exists(fk => hasFkFilterIn(j.right, fk))
    }

    private def fksInAnyPkFkJoin(condition: Expression): Seq[Attribute] = condition match {
      case EqualTo(left: Attribute, right: Attribute) if primaryKeys.contains(left.name) && foreignKeys.contains(right.name) =>
        Seq(right)
      case And(left, right) =>
        fksInAnyPkFkJoin(left) ++ fksInAnyPkFkJoin(right)
      case _ =>
        // todo: disjunction?
        Seq.empty
    }

    private def hasFkFilterIn(plan: LogicalPlan, fk: Attribute): Boolean = {
      plan.find {
        case f: Filter => isFkFilter(f)
        case _ => false
      }.isDefined
    }

    private def isFkFilter(f: Filter): Boolean = f.condition.collectLeaves().exists {
      case x: Attribute => foreignKeys.contains(x.name)
      case _ => false
    }

    override protected def dexFilterOf(predicateTable: LogicalRelation, predicateColName: String, predicateValue: Any, ridOrder: String, childView: LogicalPlan, isNegated: Boolean): LogicalPlan = {
      require(!isNegated, "todo")
      val predicateTableName = tableNameFromLogicalRelation(predicateTable)
      val predicate = dexFilterPredicate(dexFilterPredicatePrefixOf(predicateTableName, predicateColName))(predicateValue)
      // Output filtered rows in childView (eventually the source table).  Note that this is different from ridFilter
      // where output is just (value_dec_key, value) and to be joined with source table.
      // output schema: childView's schema
      // todo: projection pushdown
      // todo: add t_e column?  Can eliminnate this left semi join
      val encPredicateTableName = tableEncNameOf(predicateTableName)
      val encPredicateTable = tableEncWithRidOrderOf(predicateTable)
      // todo: for idnependent filter, can skip the childview join, but need to extend the output of the Filter operator to include also the table attributes
      if (isTableScan(childView)) {
        val labelCol = DexPrimitives.dexColNameOf(s"val_${predicateTableName}_$predicateColName")
        val labelColOrder = $"${labelCol}_${joinOrder(predicateTable)}"
        DexPseudoPrimaryKeyFilter(predicate, labelCol, labelColOrder, encPredicateTableName, childView)
      } else {
        val labelCol = DexPrimitives.dexColNameOf(s"depval_${predicateTableName}_$predicateColName")
        val labelColOrder = $"${labelCol}_${joinOrder(predicateTable)}"
        val masterTrapdoor = dexMasterTrapdoorForPred(predicate, None)
        val secondaryTrapdoor = catalystTrapdoorExprOf(masterTrapdoor, $"$ridOrder")
        childView.where(labelColOrder === secondaryTrapdoor)
      }

    }

    private def hasAttrInView(attr: String, view: LogicalPlan): Boolean = {
      analyze(view).output.exists(_.name == attr)
    }

    override protected def translateEquiJoin(joinAttrsUnordered: JoinAttrs, childViews: Seq[LogicalPlan]): LogicalPlan = {
      // ChildViews:
      // case 1: one childview => conjunctive join condition e.g. Q join T on a = b and c = d is processed as Q join T_A on ... join T_B on ...
      // case 2: two childview => singleton join condition or the leftmost leaf of conjunctive join condition
      require(childViews.size == 1 || childViews.size == 2)
      // Always join from left child view to right child view, post-orderly (subtree-first)
      // val (leftChildView, rightChildView) = (childViews.head, childViews(1))
      // order childViews by joinAttrs
      val joinAttrs =
        if (childViews.size == 2) {
          if (hasAttrInView(joinAttrsUnordered.leftRidOrder, childViews.head) && hasAttrInView(joinAttrsUnordered.rightRidOrder, childViews(1)))
            joinAttrsUnordered
          else if (hasAttrInView(joinAttrsUnordered.leftRidOrder, childViews(1)) && hasAttrInView(joinAttrsUnordered.rightRidOrder, childViews.head))
            JoinAttrs(joinAttrsUnordered.right, joinAttrsUnordered.left)
          else
            throw DexException("unmatched childviews and join attrs")
        } else if (childViews.size == 1) {
          if (hasAttrInView(joinAttrsUnordered.leftRidOrder, childViews.head))
            joinAttrsUnordered
          else if (hasAttrInView(joinAttrsUnordered.rightRidOrder, childViews.head))
            JoinAttrs(joinAttrsUnordered.right, joinAttrsUnordered.left)
          else
            throw DexException("unmatched childviews and join attrs")
        } else {
          throw DexException("childviews wrong size")
        }

          def hasFilterOn(view: LogicalPlan): Boolean = {
            view.find(x => x.isInstanceOf[DexPseudoPrimaryKeyDependentFilter] || x.isInstanceOf[DexPseudoPrimaryKeyFilter]).nonEmpty
          }

          def hasJoinOn(view: LogicalPlan): Boolean = {
            // also check Join because fk-pk join is not expressed as custom operator
            view.find(x => x.isInstanceOf[Join] || x.isInstanceOf[DexPkfkMaterializationAwareJoin]).nonEmpty
          }

          def isFilter(view: LogicalPlan): Boolean = !hasJoinOn(view) && hasFilterOn(view)


      val (leftRidOrder, rightRidOrder)= ($"${joinAttrs.leftRidOrder}", $"${joinAttrs.rightRidOrder}")
      val (leftTableAttr, rightTableAttr) = (
        TableAttributeAtom(joinAttrs.leftTableName, joinAttrs.left.name),
        TableAttributeAtom(joinAttrs.rightTableName, joinAttrs.right.name)
      )
      (leftTableAttr, rightTableAttr) match {
        case (taP, taF) if primaryKeys.contains(taP.attr) && foreignKeys.contains(taF.attr) =>
          // primary to foreign key join, e.g. supplier.s_suppkey = partsupp.ps_suppkey
          val mapColumnOrder = $"${DexPrimitives.dexColNameOf(s"fpk_${taF.table}_${taP.table}")}_${joinOrder(joinAttrs.rightTableRel)}"
          //val predicate = s"${taF.table}~${taP.table}"
          val predicate = dexPkFKJoinPredicateOf(taF, taP)
          // val mapColumnDecKey = Concat(Seq(Literal(predicate), Literal("~"), leftRidOrder))
          val mapColumnDecKey = catalystTrapdoorExprOf(dexMasterTrapdoorForPred(predicate, None), rightRidOrder)
          //val tablePrimaryKey = tableEncWithRidOrderOf(taP.table)
          // note: rids are all in bytes

        childViews match {
            case Seq(leftChildView, rightChildView) =>
              (isFilter(leftChildView), isFilter(rightChildView)) match {
                case (true, true) =>
                  val labelColumn = DexPrimitives.dexColNameOf(s"pfk_${taP.table}_${taF.table}")
                  val labelColumnOrder = $"${labelColumn}_${joinOrder(joinAttrs.rightTableRel)}"
                  val predicate = dexPkFKJoinPredicateOf(taP, taF)
                  val (taPEnc, taFEnc) = (tableEncWithRidOrderOf(joinAttrs.leftTableRel), tableEncWithRidOrderOf(joinAttrs.rightTableRel))
                  DexPkfkMaterializationAwareJoin(predicate, labelColumnOrder, leftRidOrder, NotMaterialized, NotMaterialized, leftChildView, taFEnc.select(labelColumnOrder, rightRidOrder), leftChildView.output :+ rightRidOrder)
                    .join(rightChildView, UsingJoin(Inner, Seq(joinAttrs.rightRidOrder)))
                case _ =>
                  rightChildView.join(leftChildView, condition = Some(DexDecrypt(mapColumnDecKey, mapColumnOrder) === leftRidOrder))
                    // .select(star())
              }
            case Seq(leftChildView) =>
              leftChildView.where(DexDecrypt(mapColumnDecKey, mapColumnOrder) === leftRidOrder) // .select(star())
            case _ =>
              throw DexException("longer childviews")
          }

        case (taF, taP) if foreignKeys.contains(taF.attr) && primaryKeys.contains(taP.attr) =>
          // foreign to primary key join, e.g. partsupp.ps_suppkey = supplier.s_suppkey
          val mapColumnOrder = $"${DexPrimitives.dexColNameOf(s"fpk_${taF.table}_${taP.table}")}_${joinOrder(joinAttrs.leftTableRel)}"
          //val predicate = s"${taF.table}~${taP.table}"
          val predicate = dexPkFKJoinPredicateOf(taF, taP)
          // val mapColumnDecKey = Concat(Seq(Literal(predicate), Literal("~"), leftRidOrder))
          val mapColumnDecKey = catalystTrapdoorExprOf(dexMasterTrapdoorForPred(predicate, None), leftRidOrder)
          //val tablePrimaryKey = tableEncWithRidOrderOf(taP.table)
          // note: rids are all in bytes

          childViews match {
            case Seq(leftChildView, rightChildView) =>
              (isFilter(leftChildView), isFilter(rightChildView)) match {
                case (true, true) =>
                  val labelColumn = DexPrimitives.dexColNameOf(s"pfk_${taP.table}_${taF.table}")
                  val labelColumnOrder = $"${labelColumn}_${joinOrder(joinAttrs.leftTableRel)}"
                  val predicate = dexPkFKJoinPredicateOf(taP, taF)
                  val (taPEnc, taFEnc) = (tableEncWithRidOrderOf(joinAttrs.rightTableRel), tableEncWithRidOrderOf(joinAttrs.leftTableRel))
                  DexPkfkMaterializationAwareJoin(predicate, labelColumnOrder, rightRidOrder, NotMaterialized, NotMaterialized, rightChildView, taFEnc.select(labelColumnOrder, leftRidOrder), rightChildView.output :+ leftRidOrder)
                    .join(leftChildView, UsingJoin(Inner, Seq(joinAttrs.leftRidOrder)))
                case _ =>
                  leftChildView.join(rightChildView, condition = Some(DexDecrypt(mapColumnDecKey, mapColumnOrder) === rightRidOrder))
              }
            case Seq(leftChildView) =>
              leftChildView.where(DexDecrypt(mapColumnDecKey, mapColumnOrder) === rightRidOrder) //.select(star())
            case _ =>
              throw DexException("longer childviews")
          }

        case (taL, taR) => throw DexException("unsupported: nonkey join: " + taL.toString + ", " + taR.toString)
      }
    }
  }
}

case class DexRidFilterExec(predicate: String, emm: SparkPlan) extends UnaryExecNode {

  private val labelForCounter: Long => InternalRow => Boolean = {
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
    Iterator.iterate(DexConstants.cashCounterStart)(_ + 1).map { i =>
    //Iterator.from(DexConstants.cashCounterStart).map { i =>
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
    Iterator.iterate(DexConstants.cashCounterStart)(_ + 1).map { i =>
    //Iterator.from(DexConstants.cashCounterStart).map { i =>
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

  override def output: Seq[Attribute] = left.output ++ right.output.collect {
    case x: Attribute if x.name == "label" =>
      // rename column "label"
      x.withName("value_dec_key")
    case x => x
  }
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
