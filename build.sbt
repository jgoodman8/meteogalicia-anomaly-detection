name := "meteogalicia-anomaly-detection"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}