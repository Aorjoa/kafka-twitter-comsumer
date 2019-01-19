name := "kafka"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.0"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.7"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
