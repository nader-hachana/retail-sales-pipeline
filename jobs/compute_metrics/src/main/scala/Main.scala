package com.batchpipeline.metrics

import com.batchpipeline.common.{SparkJob, CassandraIO}
import com.batchpipeline.metrics.Metrics._

object Main {
  def main(args: Array[String]): Unit = {
    println("*** PREPARING SPARK SESSION ***")
    val spark = SparkJob.session("compute-metrics")
    spark.sparkContext.setLogLevel("WARN")
    spark.catalog.clearCache()

    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "/opt/transformations")

    println("*** READING raw_sales ***")
    val sales = realSales(CassandraIO.readTable(spark, "sales", "raw_sales")).cache()
    val referencePricesDf = referencePrices(sales).cache()

    val salesWithPromo = withIsPromo(sales, referencePricesDf)

    println("*** METRIC: WEEKLY SALES ***")
    val weeklySalesDf = weeklySales(salesWithPromo).cache()
    weeklySalesDf.write.mode("overwrite").parquet(s"$outputDir/weekly_sales.parq")

    println("*** METRIC: BASELINE ***")
    val baselineDf = baseline(weeklySalesDf).cache()
    baselineDf.write.mode("overwrite").parquet(s"$outputDir/baseline.parq")

    println("*** METRIC: LIFT ***")
    val liftDf = lift(weeklySalesDf, baselineDf)
    liftDf.write.mode("overwrite").parquet(s"$outputDir/lift.parq")

    val weeklyWithForecast = withForecast(weeklySalesDf).cache()

    println("*** METRIC: SALES ERROR ***")
    val salesErrorDf = salesError(weeklyWithForecast)
    salesErrorDf.write.mode("overwrite").parquet(s"$outputDir/sales_error.parq")

    println("*** METRIC: ABSOLUTE DIFFERENCE ***")
    val absoluteDiffDf = absoluteDiff(weeklyWithForecast)
    absoluteDiffDf.write.mode("overwrite").parquet(s"$outputDir/absolute_diff.parq")

    println("*** METRIC: PROFIT ERROR ***")
    val profitErrorDf = profitError(weeklyWithForecast, referencePricesDf)
    profitErrorDf.write.mode("overwrite").parquet(s"$outputDir/profit_error.parq")

    println("*** METRIC: PRICE HISTORY ***")
    val priceHistoryDf = priceHistory(sales)
    priceHistoryDf.write.mode("overwrite").parquet(s"$outputDir/price_history.parq")

    println("*** STOPPING SPARK SESSION ***")
    spark.stop()
  }
}
