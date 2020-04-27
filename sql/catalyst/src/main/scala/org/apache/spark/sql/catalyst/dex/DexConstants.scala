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
package org.apache.spark.sql.catalyst.dex
// scalastyle:off

object DexConstants {
  val cashCounterStart: Long = 0L
  val tFilterName = "t_filter"
  val tDepFilterName = "t_depfilter"
  val tCorrJoinName = "t_correlated_join"
  val tUncorrJoinName = "t_uncorrelated_join"
  val tDomainName = "t_domain"
  val emmLabelCol = "label"
  val emmValueCol = "value"
  val emmValueDecKeyCol = "value_dec_key"
  val ridCol = "rid"
  val tDepFilterCol = "label"

  type TableName = String
  type AttrName = String
  type JoinableAttrs = (TableAttribute, TableAttribute)

  sealed trait TableAttribute {
    def table: TableName
    def attr: AttrName
    def qualifiedName: String = table + "." + attr
  }
  case class TableAttributeAtom(table: TableName, attr: AttrName) extends TableAttribute
  case class TableAttributeCompound(table: TableName, attrs: Seq[AttrName]) extends TableAttribute {
    override def attr: AttrName = attrs.mkString("_and_")
  }
}
