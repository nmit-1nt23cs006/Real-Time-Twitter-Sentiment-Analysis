name := "TwitterSentimentAnalysis"
version := "1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming"      % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

// Avoid merge conflicts when building fat jar
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
