package com.batchpipeline.ingest

import com.batchpipeline.common.SparkJob
import com.batchpipeline.ingest.Utils.loadToDb

object Main {
  def main(args: Array[String]): Unit = {
    println("*** PREPARING SPARK SESSION ***")
    val spark = SparkJob.session("ingest-raw-data")
    spark.sparkContext.setLogLevel("WARN")
    spark.catalog.clearCache()

    val dataDir = "/data/raw/"
    loadToDb(spark, dataDir)

    println("*** STOPPING SPARK SESSION ***")
    spark.stop()
  }
}
