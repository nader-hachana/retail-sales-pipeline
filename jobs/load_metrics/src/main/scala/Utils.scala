package com.batchpipeline.load

import org.apache.spark.sql._

object Utils {
  val keyspace = "sales_metrics"
  val tables = Seq(
    "weekly_sales",
    "baseline",
    "lift",
    "sales_error",
    "absolute_diff",
    "profit_error",
    "price_history"
  )

  // every run recomputes the full metrics history from scratch, overwrite
  // keeps a rerun from piling up stale duplicate rows next to fresh ones
  def loadToDb(spark: SparkSession, outputDir: String): Unit = {
    for (table <- tables) {
      val df = spark.read.parquet(s"$outputDir/$table.parq")

      println(s"*** LOADING $table.parq INTO $keyspace.$table (${df.count()} rows) ***")

      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> table, "keyspace" -> keyspace, "confirm.truncate" -> "true"))
        .mode("overwrite")
        .save()
    }
  }
}
