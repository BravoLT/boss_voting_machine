name := "Result"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0"
)


// for debugging sbt problems
logLevel := Level.Debug

scalacOptions += "-deprecation"
