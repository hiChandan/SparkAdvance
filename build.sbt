name := "SparkAdvance"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0" exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql" % "3.0.0" exclude ("org.slf4j", "slf4j-log4j12"),
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "ch.qos.logback" % "logback-classic" % "1.2.10"
)
