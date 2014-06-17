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
  "org.apache.spark" %% "spark-core" % "1.0.0" excludeAll(
    ExclusionRule(name = "slf4j-log4j12"),
    ExclusionRule(name = "log4j"),
    ExclusionRule(name = "jackson-databind")),
  "org.apache.spark" %% "spark-graphx" % "1.0.0" excludeAll(
    ExclusionRule(name = "slf4j-log4j12"),
    ExclusionRule(name = "log4j"),
    ExclusionRule(name = "jackson-databind")),
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
  "org.pegdown" % "pegdown" % "1.4.2" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.6")

play.Project.playScalaSettings

// org.scalatest does not take the same testOptions as the one included in Play!
testOptions in Test := Nil

// write sbt test stdout to file
testOptions in Test += Tests.Argument("-fWDF", "logs/sbttest.out")

// additional fancy html reporting using pegdown (todo: parboiled throws an exception now)
// testOptions in Test += Tests.Argument("-h", "logs/html")

// dependency graph visualizer setting, usage example 'sbt dependency-tree'
net.virtualvoid.sbt.graph.Plugin.graphSettings

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(scalariform.formatter.preferences.AlignParameters, true)
  .setPreference(scalariform.formatter.preferences.DoubleIndentClassDeclaration, true)
