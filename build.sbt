lazy val sparkVersion = "3.5.1"
lazy val cassandraConnectorVersion = "3.5.1"
lazy val scalaTestVersion = "3.2.19"

lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.12.19",
  organization := "com.batchpipeline",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  )
)

lazy val assemblySettings = Seq(
  assembly / logLevel := Level.Error,
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x                             => MergeStrategy.first
  }
)

// shared Spark session setup, every job depends on this instead of
// duplicating its own SparkConf/SparkSession boilerplate
lazy val common = (project in file("jobs/common"))
  .settings(commonSettings)
  .settings(
    name := "common",
    // scalatest is a real compile-time dependency here, not just a test one,
    // since SparkTestBase is a reusable test utility every other job imports
    libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion
  )

lazy val ingestRawData = (project in file("jobs/ingest_raw_data"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    name := "ingest-raw-data",
    assembly / assemblyJarName := "ingest_raw_data.jar",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion
  )

lazy val validateRawData = (project in file("jobs/validate_raw_data"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    name := "validate-raw-data",
    assembly / assemblyJarName := "validate_raw_data.jar",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion
  )

lazy val computeMetrics = (project in file("jobs/compute_metrics"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    name := "compute-metrics",
    assembly / assemblyJarName := "compute_metrics.jar",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion
  )

lazy val loadMetrics = (project in file("jobs/load_metrics"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    name := "load-metrics",
    assembly / assemblyJarName := "load_metrics.jar",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion
  )

lazy val root = (project in file("."))
  .aggregate(common, ingestRawData, validateRawData, computeMetrics, loadMetrics)
  .settings(
    scalaVersion := "2.12.19",
    publish / skip := true
  )
