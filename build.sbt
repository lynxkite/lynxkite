name := "biggraph"

javaOptions in Test := Seq(
  "-Dsun.io.serialization.extendedDebugInfo=true",
  "-Dbiggraph.default.partitions.per.core=1")

scalacOptions ++= Seq("-feature", "-deprecation")

version := "0.1-SNAPSHOT"

sources in doc in Compile := List()  // Disable doc generation.

publishArtifact in packageSrc := false  // Don't package source.

scalaVersion := "2.10.4"

ivyXML := <dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="2.5.0.v201103041518"><artifact name="javax.servlet" type="orbit" ext="jar"/></dependency> // eclipse needs this

libraryDependencies ++= Seq(
  jdbc,
  anorm,
  cache,
  "com.typesafe.akka" %% "akka-actor" % "2.2.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.2.3",
  "org.apache.commons" % "commons-lang3" % "3.3",
  "org.apache.spark" %% "spark-core" % "1.0.1" excludeAll(
    ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"),
    ExclusionRule(organization = "log4j", name = "log4j"),
    ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-databind")),
  "ws.securesocial" %% "securesocial" % "2.1.4",
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
  "org.pegdown" % "pegdown" % "1.4.2" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.6")

play.Project.playScalaSettings

// Save logs to a file. Do not run benchmarks by default. (Use "sbt bench:test" to run them.)
testOptions in Test := Seq(Tests.Argument("-fWDF", "logs/sbttest.out", "-l", "Benchmark"))

// dependency graph visualizer setting, usage example 'sbt dependency-tree'
net.virtualvoid.sbt.graph.Plugin.graphSettings

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(scalariform.formatter.preferences.AlignParameters, true)
  .setPreference(scalariform.formatter.preferences.DoubleIndentClassDeclaration, true)

// Separate config for benchmarks.
lazy val Benchmark = (config("bench") extend Test)

inConfig(Benchmark)(Defaults.testTasks) ++ Seq(
  testOptions in Benchmark := Seq(Tests.Argument("-n", "Benchmark"))
)

lazy val root = project.in(file(".")).configs(Benchmark)
