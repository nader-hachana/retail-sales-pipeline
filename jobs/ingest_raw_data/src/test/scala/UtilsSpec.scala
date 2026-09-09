package com.batchpipeline.ingest

import com.batchpipeline.common.SparkTestBase
import org.apache.spark.sql.types.{StringType, TimestampType, DateType}

class UtilsSpec extends SparkTestBase {
  test("applyKnownCasts casts invoice_date to timestamp and date to date, leaves other columns alone") {
    val ss = spark
    import ss.implicits._
    val df = Seq(("85123A", "2010-12-01 08:26:00", "2010-12-01")).toDF("sku", "invoice_date", "date")

    assert(df.schema("invoice_date").dataType == StringType)

    val result = Utils.applyKnownCasts(df)

    assert(result.schema("invoice_date").dataType == TimestampType)
    assert(result.schema("date").dataType == DateType)
    assert(result.schema("sku").dataType == StringType, "sku was never listed as a cast target, should stay untouched")
    assert(result.count() == 1)
  }

  test("applyKnownCasts is a no-op when none of the known columns are present") {
    val ss = spark
    import ss.implicits._
    val df = Seq(("85123A", 6)).toDF("sku", "quantity")

    val result = Utils.applyKnownCasts(df)

    assert(result.schema == df.schema)
  }
}
