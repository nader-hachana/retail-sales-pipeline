package com.batchpipeline.validate

import com.batchpipeline.common.SparkJob
import com.batchpipeline.validate.Utils.qualityCheck
import com.batchpipeline.validate.exceptions.QualityException

object Main {
  def main(args: Array[String]): Unit = {
    println("*** PREPARING SPARK SESSION ***")
    val spark = SparkJob.session("validate-raw-data")
    spark.sparkContext.setLogLevel("WARN")
    spark.catalog.clearCache()

    val configFile = "/config/config.csv"

    try {
      qualityCheck(spark, configFile)
      println("*** ALL CHECKS PASSED ***")
    } catch {
      case e: QualityException =>
        println(s"*** QUALITY CHECK FAILED: ${e.getMessage} ***")
        spark.stop()
        System.exit(1)
    }

    println("*** STOPPING SPARK SESSION ***")
    spark.stop()
  }
}
