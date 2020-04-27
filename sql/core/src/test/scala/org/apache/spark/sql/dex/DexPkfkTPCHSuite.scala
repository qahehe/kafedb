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
package org.apache.spark.sql.dex
// scalastyle:off

class DexPkfkTPCHSuite extends DexPkfkTPCHTest {

  test("one filter") {
    val query = part.where("p_name == 'pb'").select("p_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two conjunctive filters") {
    val query = supplier.where("s_name == 'sb' and s_address == 'sa1'").select("s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one join: foreign key to primary key") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey").select("ps_comment", "s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one join: primary key to foreign key") {
    val query = supplier.join(partsupp).where("s_suppkey == ps_suppkey").select("ps_comment", "s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one pk-fk join one filter on pk table") {
    val query = supplier.join(partsupp).where("s_name == 'sb' and s_suppkey == ps_suppkey").select("ps_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one pk-fk join one filter on fk table") {
    val query = supplier.join(partsupp).where("s_suppkey == ps_suppkey and ps_comment == 'psb'").select("s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one fk-pk join one filter on pk table") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey and s_name == 'sa'").select("ps_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("one fk-pk join one filter on fk table") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey and ps_comment == 'psb'").select("s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: fk-pk and fk-pk") {
    val query = partsupp.join(supplier).where("ps_suppkey == s_suppkey").join(part).where("ps_partkey == p_partkey").select("s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: pk-fk and pk-fk") {
    val query = supplier.join(part.join(partsupp).where("p_partkey == ps_partkey")).where("s_suppkey == ps_suppkey").select("s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: fk-pk and pk-fk") {
    val query = part.join(partsupp.join(supplier).where("ps_suppkey == s_suppkey")).where("p_partkey == ps_partkey").select("s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("two joins: pk-fk and fk-pk") {
    val query = part.join(partsupp).where("p_partkey == ps_partkey").join(supplier).where("ps_suppkey == s_suppkey").select("s_name")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("compound key join: pk-fk") {
    val query = partsupp.join(lineitem).where("ps_partkey = l_partkey and ps_suppkey = l_suppkey").select("ps_comment", "l_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("compound key join: fk-pk") {
    val query = lineitem.join(partsupp).where("l_partkey = ps_partkey and l_suppkey = ps_suppkey").select("ps_comment", "l_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("partial compound fk in fk-pk join") {
    val query = lineitem.join(part).where("l_partkey = p_partkey").select("l_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("partial compound fk in pk-fk join") {
    val query = part.join(lineitem).where("p_partkey = l_partkey").select("l_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("partial compound pk in fk-pk join") {
    val query = partsupp.join(part).where("ps_partkey == p_partkey").select("ps_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("partial compound pk in pk-fk join") {
    val query = partsupp.join(part).where("p_partkey == ps_partkey").select("ps_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("join out of predicate order") {
    val query = partsupp.join(part).where("ps_partkey == p_partkey").join(supplier).where("s_suppkey = ps_suppkey").select("ps_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("right depth tree unfiltereds") {
    val query = supplier.join(partsupp.join(part).where("ps_partkey == p_partkey")).where("s_suppkey = ps_suppkey").select("ps_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

  test("right depth tree filtered") {
    val query = supplier.join(partsupp.join(part).where("p_partkey == ps_partkey")).where("s_name == 'sb' and s_suppkey = ps_suppkey").select("ps_comment")
    checkDexFor(query, query.dexPkFk(pks, fks))
  }

}
