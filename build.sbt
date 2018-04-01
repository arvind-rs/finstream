name := "Financial Streaming Datapipeline"

version := "1.0"

scalaVersion := "2.11.8"

fork := true

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.3.0",
	"org.apache.spark" %% "spark-sql" % "2.3.0",
	"org.apache.spark" %% "spark-streaming" % "2.3.0"
) 