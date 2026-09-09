package com.batchpipeline.validate

import com.batchpipeline.common.SparkTestBase
import com.batchpipeline.validate.exceptions.{CompletenessException, IncorrectnessException}

class UtilsSpec extends SparkTestBase {
  test("completenessCheck only warns, doesn't fail, on a missing column that isn't marked critical") {
    // real finding this models: ~0.4% of real rows have a missing
    // description, a genuine data issue, but nothing downstream uses that
    // column, failing the whole pipeline over it would be too strict
    val ss = spark
    import ss.implicits._
    val df = Seq(("85123A", None: Option[String])).toDF("sku", "description")

    noException should be thrownBy Utils.completenessCheck(df, "raw_sales", "sales", criticalColumns = Set.empty)
  }

  test("completenessCheck fails when a column marked critical is missing") {
    val ss = spark
    import ss.implicits._
    val df = Seq(("85123A", None: Option[Double])).toDF("sku", "unit_price")

    an[CompletenessException] should be thrownBy
      Utils.completenessCheck(df, "raw_sales", "sales", criticalColumns = Set("unit_price"))
  }

  test("completenessCheck excludes key columns entirely, real bug: Cassandra rejects an empty-string " +
    "comparison pushed down against a key column (partition or clustering) with 'Key may not be empty'") {
    val ss = spark
    import ss.implicits._
    // invoice_date is a real clustering key, deliberately null here to prove
    // the exclusion itself works, not that a null could really occur there
    val df = Seq(("85123A", None: Option[String])).toDF("sku", "invoice_date")

    noException should be thrownBy
      Utils.completenessCheck(df, "raw_sales", "sales", keyColumns = Set("sku", "invoice_date"))
  }

  test("salesIncorrectnessCheck passes for a normal sale and a real cancellation") {
    val ss = spark
    import ss.implicits._
    val df = Seq(
      ("489434", "85123A", 6, 2.55),   // normal sale, positive quantity
      ("C489435", "85123A", -6, 2.55), // real cancellation, negative quantity expected
    ).toDF("invoice_id", "sku", "quantity", "unit_price")

    noException should be thrownBy Utils.salesIncorrectnessCheck(df)
  }

  test("salesIncorrectnessCheck fails on a negative quantity outside a cancellation") {
    val ss = spark
    import ss.implicits._
    val df = Seq(("489434", "85123A", -6, 2.55)).toDF("invoice_id", "sku", "quantity", "unit_price")

    an[IncorrectnessException] should be thrownBy Utils.salesIncorrectnessCheck(df)
  }

  test("salesIncorrectnessCheck fails on a negative price") {
    val ss = spark
    import ss.implicits._
    val df = Seq(("489434", "85123A", 6, -2.55)).toDF("invoice_id", "sku", "quantity", "unit_price")

    an[IncorrectnessException] should be thrownBy Utils.salesIncorrectnessCheck(df)
  }

  test("salesIncorrectnessCheck passes for a real stock adjustment (0 price, non-positive quantity, not a cancellation)") {
    // real rows found in the actual data: description "damages", "lost",
    // "mixed", always unit_price=0, a distinct record type, not a broken sale
    val ss = spark
    import ss.implicits._
    val df = Seq(("489521", "21646", -50, 0.0)).toDF("invoice_id", "sku", "quantity", "unit_price")

    noException should be thrownBy Utils.salesIncorrectnessCheck(df)
  }

  test("salesIncorrectnessCheck passes for a real bad-debt write-off (sku B, negative price)") {
    val ss = spark
    import ss.implicits._
    val df = Seq(("A506401", "B", 1, -53594.36)).toDF("invoice_id", "sku", "quantity", "unit_price")

    noException should be thrownBy Utils.salesIncorrectnessCheck(df)
  }
}
