package com.batchpipeline.metrics

import com.batchpipeline.common.SparkTestBase
import java.sql.Timestamp

class MetricsSpec extends SparkTestBase {
  def ts(s: String): Timestamp = Timestamp.valueOf(s)

  test("realSales keeps cancellations alongside their sale but drops adjustments and write-offs") {
    val ss = spark
    import ss.implicits._
    val df = Seq(
      ("1", "A", 5, 2.0),   // a real sale
      ("C1", "A", -5, 2.0), // a real cancellation, kept so it nets out downstream
      ("2", "A", -3, 0.0),  // a real stock adjustment
      ("3", "A", 4, 0.0),   // zero price, not a real sale either
      ("4", "B", -1, -1.0), // a bad-debt write-off
    ).toDF("invoice_id", "sku", "quantity", "unit_price")

    val result = Metrics.realSales(df)
    result.select("invoice_id").as[String].collect() should contain theSameElementsAs Seq("1", "C1")
  }

  test("referencePrices is a sku's median real unit_price") {
    val ss = spark
    import ss.implicits._
    val sales = Seq(("A", 10.0), ("A", 8.0), ("A", 9.0)).toDF("sku", "unit_price")

    val result = Metrics.referencePrices(sales)
    result.select("reference_price").as[Double].collect().head shouldBe 9.0
  }

  test("withIsPromo flags a price meaningfully below the sku's real median price") {
    val ss = spark
    import ss.implicits._
    val sales = Seq(("A", 10.0), ("A", 8.0)).toDF("sku", "unit_price")
    val referencePrices = Seq(("A", 10.0)).toDF("sku", "reference_price")

    val result = Metrics.withIsPromo(sales, referencePrices).orderBy("unit_price")
    val flags = result.select("is_promo").as[Boolean].collect()
    flags should contain theSameElementsInOrderAs Seq(true, false) // 8.0 then 10.0
  }

  test("weeklySales groups same-week transactions together and keeps different weeks separate") {
    val ss = spark
    import ss.implicits._
    val df = Seq(
      ("A", ts("2010-12-01 08:00:00"), 5, false), // week of Nov 29
      ("A", ts("2010-12-03 10:00:00"), 3, true),  // same week, has a promo
      ("A", ts("2010-12-08 09:00:00"), 2, false), // next week (Dec 6)
    ).toDF("sku", "invoice_date", "quantity", "is_promo")

    val result = Metrics.weeklySales(df).orderBy("week")
    val rows = result.collect()
    rows.length shouldBe 2
    rows(0).getAs[Long]("weekly_sales") shouldBe 8
    rows(0).getAs[Boolean]("had_promo") shouldBe true
    rows(1).getAs[Long]("weekly_sales") shouldBe 2
    rows(1).getAs[Boolean]("had_promo") shouldBe false
  }

  test("baseline is the median of non-promotional weeks only") {
    val ss = spark
    import ss.implicits._
    val weekly = Seq(
      ("A", ts("2010-11-01 00:00:00"), 10L, false),
      ("A", ts("2010-11-08 00:00:00"), 11L, false),
      ("A", ts("2010-11-15 00:00:00"), 12L, false),
      ("A", ts("2010-11-22 00:00:00"), 50L, true), // a promo week, excluded
    ).toDF("sku", "week", "weekly_sales", "had_promo")

    val result = Metrics.baseline(weekly)
    result.select("baseline").as[Double].collect().head shouldBe 11.0
  }

  test("lift compares a promotional week's actual sales against the baseline") {
    val ss = spark
    import ss.implicits._
    val weekly = Seq(("A", ts("2010-11-22 00:00:00"), 15L, true)).toDF("sku", "week", "weekly_sales", "had_promo")
    val baseline = Seq(("A", 10.0)).toDF("sku", "baseline")

    val row = Metrics.lift(weekly, baseline).collect().head
    row.getAs[Double]("lift_unit") shouldBe 5.0
    row.getAs[Double]("lift_percentage") shouldBe 1.5
  }

  test("withForecast averages exactly the trailing 4 weeks, nothing more, nothing less") {
    val ss = spark
    import ss.implicits._
    val weekly = Seq(
      ("A", ts("2010-11-01 00:00:00"), 10L),
      ("A", ts("2010-11-08 00:00:00"), 20L),
      ("A", ts("2010-11-15 00:00:00"), 30L),
      ("A", ts("2010-11-22 00:00:00"), 40L),
      ("A", ts("2010-11-29 00:00:00"), 50L),
    ).toDF("sku", "week", "weekly_sales")

    val result = Metrics.withForecast(weekly).orderBy("week").collect()
    result(0).isNullAt(result(0).fieldIndex("forecast")) shouldBe true // no prior weeks yet
    result(4).getAs[Double]("forecast") shouldBe 25.0 // avg(10,20,30,40)
  }

  test("profitError scales the sales error by the sku's real reference price") {
    val ss = spark
    import ss.implicits._
    val weekly = Seq(("A", ts("2010-11-22 00:00:00"), 20L, 10.0)).toDF("sku", "week", "weekly_sales", "forecast")
    val referencePrices = Seq(("A", 5.0)).toDF("sku", "reference_price")

    val row = Metrics.profitError(weekly, referencePrices).collect().head
    row.getAs[Double]("profit_error") shouldBe 0.5 // |20*5 - 10*5| / |20*5|
  }

  test("priceHistory keeps only real price changes, not repeated identical prices") {
    val ss = spark
    import ss.implicits._
    val df = Seq(
      ("A", ts("2010-12-01 00:00:00"), 10.0),
      ("A", ts("2010-12-02 00:00:00"), 10.0), // same price, not a real change
      ("A", ts("2010-12-03 00:00:00"), 8.0),  // a real price change
    ).toDF("sku", "invoice_date", "unit_price")

    val history = Metrics.priceHistory(df).collect().head.getAs[Seq[Any]]("price_history")
    history.length shouldBe 2
  }
}
