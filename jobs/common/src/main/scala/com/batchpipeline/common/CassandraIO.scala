package com.batchpipeline.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object CassandraIO {
  def readTable(spark: SparkSession, keyspace: String, table: String): DataFrame =
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load()
}
