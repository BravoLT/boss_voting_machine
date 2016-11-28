name := "Result"

version := "1.0"

scalaVersion := "2.11.6"

assemblyJarName in assembly := "blah.jar"

resolvers in Global ++= Seq(
  "Sbt plugins"                   at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server"          at "http://repo1.maven.org/maven2",
  "TypeSafe Repository Releases"  at "http://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case x =>
    val baseStrategy = (assemblyMergeStrategy in assembly).value
    baseStrategy(x)
}

// for debugging sbt problems
logLevel := Level.Debug

scalacOptions += "-deprecation"
