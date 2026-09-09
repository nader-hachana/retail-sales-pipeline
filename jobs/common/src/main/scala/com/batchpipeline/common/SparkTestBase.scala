package com.batchpipeline.common

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Base trait every job's tests extend, one real local SparkSession shared
  * across a test suite instead of every job managing its own.
  */
trait SparkTestBase extends AnyFunSuite with BeforeAndAfterAll with Matchers {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("test").master("local[1]").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }
}
