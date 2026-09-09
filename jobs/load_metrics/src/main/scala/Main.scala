package com.batchpipeline.load

import com.batchpipeline.common.SparkJob
import com.batchpipeline.load.Utils.loadToDb

object Main {
  def main(args: Array[String]): Unit = {
    println("*** PREPARING SPARK SESSION ***")
    val spark = SparkJob.session("load-metrics")
    spark.sparkContext.setLogLevel("WARN")
    spark.catalog.clearCache()

    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "/opt/transformations")
    loadToDb(spark, outputDir)

    println("*** STOPPING SPARK SESSION ***")
    spark.stop()
  }
}
