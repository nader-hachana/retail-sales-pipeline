package com.batchpipeline.validate

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer
import com.batchpipeline.validate.exceptions.{CompletenessException, IncorrectnessException}

object Utils {
  // key columns (partition or clustering) can never be null or empty for a
  // row that exists at all, Cassandra can't store one without a valid key.
  // Checking them is pointless, and comparing one to an empty string fails
  // server-side once Spark pushes the filter down ("Key may not be empty").
  def completenessCheck(
      df: DataFrame,
      table: String,
      keyspace: String,
      keyColumns: Set[String] = Set.empty,
      criticalColumns: Set[String] = Set.empty
  ): Unit = {
    val columnsToCheck = df.schema.fields.filterNot(f => keyColumns.contains(f.name))
    val missing = ListBuffer[(String, Long)]()
    for (field <- columnsToCheck) {
      val condition =
        if (field.dataType == StringType) col(field.name).isNull || col(field.name) === ""
        else col(field.name).isNull
      val missingCount = df.filter(condition).count()
      if (missingCount > 0) missing.prepend((field.name, missingCount))
    }

    if (missing.isEmpty) {
      println(s"*** completeness OK for $keyspace.$table ***")
      return
    }

    val (critical, informational) = missing.partition { case (c, _) => criticalColumns.contains(c) }
    informational.foreach { case (c, n) =>
      println(s"*** WARN: $keyspace.$table.$c has $n missing values, not critical to downstream metrics, continuing ***")
    }
    if (critical.nonEmpty) {
      val msg = critical.map { case (c, n) => s"$c has $n missing values" }.mkString(", ")
      throw new CompletenessException(s"completeness check failed for $keyspace.$table: $msg")
    }
  }

  // A cancellation invoice (id starting with C) is expected to carry a
  // negative quantity, a real, known convention of this dataset. Rows with
  // unit_price = 0 and quantity <= 0 are a different, legitimate record
  // type entirely, manual stock adjustments (descriptions like "damages",
  // "lost", "mixed"), not broken sales, so they're excluded here rather
  // than flagged. sku "B" is specifically "Adjust bad debt", a real
  // accounting write-off, the only known source of a genuine negative
  // price, confirmed directly against the real data, not assumed.
  def salesIncorrectnessCheck(df: DataFrame): Unit = {
    val badQuantity = df
      .filter(!col("invoice_id").startsWith("C"))
      .filter(col("quantity") <= 0)
      .filter(col("unit_price") =!= 0.0)
      .count()
    val badPrice = df.filter(col("unit_price") < 0).filter(col("sku") =!= "B").count()

    if (badQuantity == 0 && badPrice == 0) {
      println("*** sales incorrectness check OK ***")
    } else {
      val parts = ListBuffer[String]()
      if (badQuantity > 0) parts += s"$badQuantity non-cancellation rows with quantity <= 0"
      if (badPrice > 0) parts += s"$badPrice rows with a negative price"
      throw new IncorrectnessException(parts.mkString(", "))
    }
  }

  def qualityCheck(spark: SparkSession, configFile: String): Unit = {
    val configDF = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv(configFile)

    configDF.collect().foreach { row =>
      val keyspace = row.getAs[String]("keyspace")
      val table = row.getAs[String]("table")
      val keyColumns = row.getAs[String]("key_columns").split(",").toSet
      val criticalColumns = row.getAs[String]("critical_columns").split(",").toSet

      println(s"*** VALIDATING $keyspace.$table ***")
      val loaded = com.batchpipeline.common.CassandraIO.readTable(spark, keyspace, table)

      completenessCheck(loaded, table, keyspace, keyColumns, criticalColumns)
      if (table == "raw_sales") salesIncorrectnessCheck(loaded)
    }
  }
}
