package com.batchpipeline.ingest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Utils {
  // columns that need an explicit cast, Spark's CSV inferSchema reads them
  // as strings, but Cassandra expects real timestamp/date types
  val timestampColumns = Set("invoice_date", "first_seen", "last_seen")
  val dateColumns = Set("date")

  def applyKnownCasts(df: DataFrame): DataFrame = {
    var result = df
    for (c <- result.columns if timestampColumns.contains(c)) {
      result = result.withColumn(c, col(c).cast("timestamp"))
    }
    for (c <- result.columns if dateColumns.contains(c)) {
      result = result.withColumn(c, col(c).cast("date"))
    }
    result
  }

  case class SourceFile(fileName: String, table: String)

  val keyspace = "sales"
  val sourceFiles = Seq(
    SourceFile("sales.csv", "raw_sales"),
    SourceFile("products.csv", "raw_products"),
    SourceFile("calendar.csv", "raw_calendar")
  )

  def loadToDb(spark: SparkSession, dataDir: String): Unit = {
    for (source <- sourceFiles) {
      val raw = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"$dataDir${source.fileName}")
      val df = applyKnownCasts(raw)

      println(s"*** LOADING ${source.fileName} INTO $keyspace.${source.table} (${df.count()} rows) ***")

      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> source.table, "keyspace" -> keyspace))
        .mode("append")
        .save()
    }
  }
}
