name := "SparkScalaSBX"

version := "0.1"

scalaVersion := "2.11.8"

lazy val excludeJars = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
val sparkVersion = "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion excludeAll(excludeJars)

