package com.batchpipeline.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Shared Spark session setup, every job in this pipeline connects to the
  * same local Cassandra the same way, this was previously duplicated in
  * each job's own Main.scala.
  */
object SparkJob {
  def session(appName: String): SparkSession = {
    val conf: SparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", sys.env.getOrElse("CASSANDRA_HOST", "cassandra"))
      .set("spark.cassandra.connection.port", sys.env.getOrElse("CASSANDRA_PORT", "9042"))

    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config(conf)
      .getOrCreate()
  }
}
