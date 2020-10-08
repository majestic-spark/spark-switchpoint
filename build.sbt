organization := "com.github.majestic"
name := "spark-switchpoint"

version := "0.1"

scalaVersion := "2.12.8"
version := "0.21.3"
val sparkVersion = "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"

