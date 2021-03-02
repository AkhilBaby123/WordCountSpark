name := "SparkTraining"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.test")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.2"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % Test

