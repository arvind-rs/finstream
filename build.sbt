name := "Financial Streaming Datapipeline"

version := "1.0"

scalaVersion := "2.11.8"

fork := true

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.3.0",
	"org.apache.spark" %% "spark-sql" % "2.3.0",
	"org.apache.spark" %% "spark-streaming" % "2.3.0",
	"com.typesafe" % "config" % "1.3.1",
	"commons-io" % "commons-io" % "2.5",
	"net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.6.1",
	"org.json" % "json" % "20160810"
) 