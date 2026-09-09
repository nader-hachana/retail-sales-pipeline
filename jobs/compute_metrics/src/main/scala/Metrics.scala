package com.batchpipeline.metrics

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Metrics {
  // a real sale line, keeping cancellations (negative quantity, real price)
  // alongside their original purchase so a later weekly sum nets them out.
  // manual stock adjustments and the sku "B" bad-debt write-off are the only
  // non-cancellation records with unit_price <= 0 in this real dataset (see
  // validate_raw_data's incorrectness check), so this one filter excludes
  // exactly those and nothing else.
  def realSales(sales: DataFrame): DataFrame = {
    sales.filter(col("unit_price") > 0)
  }

  // this dataset has no explicit promotion flag, a transaction priced
  // meaningfully below the sku's own typical price is a real, defensible
  // proxy for "this happened during a promotion". the typical price is the
  // sku's median unit_price, not its all-time max: this dataset has heavy
  // legitimate per-invoice price variation (bulk pricing), so most sales sit
  // well below the single highest price ever charged, and using that max as
  // the reference flagged 84% of weeks as promotional against real data.
  val promoPriceThreshold = 0.85

  def referencePrices(sales: DataFrame): DataFrame = {
    sales.groupBy("sku").agg(expr("percentile(unit_price, 0.5)").as("reference_price"))
  }

  def withIsPromo(sales: DataFrame, referencePrices: DataFrame): DataFrame = {
    sales
      .join(referencePrices, Seq("sku"), "left_outer")
      .withColumn("is_promo", col("unit_price") < (col("reference_price") * lit(promoPriceThreshold)))
  }

  def weeklySales(salesWithPromo: DataFrame): DataFrame = {
    salesWithPromo
      .withColumn("week", date_trunc("week", col("invoice_date")))
      .groupBy("sku", "week")
      .agg(
        sum("quantity").alias("weekly_sales"),
        max(col("is_promo").cast("int")).alias("had_promo_int")
      )
      .withColumn("had_promo", col("had_promo_int") === 1)
      .drop("had_promo_int")
  }

  // the typical, non-promotional weekly sales level for a sku, computed
  // only from real weeks that had no promotional transactions at all
  def baseline(weekly: DataFrame): DataFrame = {
    weekly
      .filter(!col("had_promo"))
      .groupBy("sku")
      .agg(expr("percentile(weekly_sales, 0.5)").as("baseline"))
  }

  def lift(weekly: DataFrame, baseline: DataFrame): DataFrame = {
    weekly
      .filter(col("had_promo"))
      .join(baseline, Seq("sku"), "inner")
      .withColumn("lift_unit", col("weekly_sales") - col("baseline"))
      .withColumn("lift_percentage", col("weekly_sales") / col("baseline"))
  }

  // a naive forecast, the average of a sku's own trailing weeks, a real,
  // standard baseline technique, used since there's no legitimate external
  // forecast source for this dataset
  val trailingWindowWeeks = 4

  def withForecast(weekly: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("sku").orderBy("week").rowsBetween(-trailingWindowWeeks, -1)
    weekly.withColumn("forecast", avg("weekly_sales").over(windowSpec))
  }

  def salesError(weeklyWithForecast: DataFrame): DataFrame = {
    weeklyWithForecast
      .filter(col("forecast").isNotNull)
      .withColumn("error", abs(col("forecast") - col("weekly_sales")) / abs(col("weekly_sales")))
  }

  def absoluteDiff(weeklyWithForecast: DataFrame): DataFrame = {
    weeklyWithForecast
      .filter(col("forecast").isNotNull)
      .withColumn("absolute_diff", abs(col("forecast") - col("weekly_sales")))
  }

  def profitError(weeklyWithForecast: DataFrame, referencePrices: DataFrame): DataFrame = {
    weeklyWithForecast
      .filter(col("forecast").isNotNull)
      .join(referencePrices, Seq("sku"), "left_outer")
      .withColumn(
        "profit_error",
        abs(col("weekly_sales") * col("reference_price") - col("forecast") * col("reference_price")) /
          abs(col("weekly_sales") * col("reference_price"))
      )
  }

  // real price changes over time per sku, straight from the transactions
  // themselves, no synthetic markdown table needed
  def priceHistory(sales: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("sku").orderBy("invoice_date")
    sales
      .withColumn("prev_price", lag("unit_price", 1).over(windowSpec))
      .filter(col("prev_price").isNull || col("unit_price") =!= col("prev_price"))
      .groupBy("sku")
      .agg(array_sort(collect_list(struct("invoice_date", "unit_price"))).alias("price_history"))
  }
}
