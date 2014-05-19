name := "biggraph"

version := "0.1-SNAPSHOT"

sources in doc in Compile := List()  // Disable doc generation.

publishArtifact in packageSrc := false  // Don't package source.

scalaVersion := "2.10.4"

ivyXML := <dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="2.5.0.v201103041518"><artifact name="javax.servlet" type="orbit" ext="jar"/></dependency> // eclipse needs this

// We might need something like these:
//  "com.typesafe.akka" %% "akka-actor" % "2.2.3",
//  "com.typesafe.akka" %% "akka-slf4j" % "2.2.3",

libraryDependencies ++= Seq(
  jdbc,
  anorm,
  cache,
  "com.typesafe.akka" %% "akka-actor" % "2.2.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.2.3",
  "org.apache.spark" %% "spark-core" % "0.9.1" excludeAll(
    ExclusionRule(name = "slf4j-log4j12"),
    ExclusionRule(name = "log4j")),
  "org.apache.spark" %% "spark-graphx" % "0.9.1" excludeAll(
    ExclusionRule(name = "slf4j-log4j12"),
    ExclusionRule(name = "log4j")),
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
  "org.pegdown" % "pegdown" % "1.4.2" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.6")

play.Project.playScalaSettings

// org.scalatest does not take the same testOptions as the one included in Play!
testOptions in Test := Nil

testOptions in Test += Tests.Argument("-fWDF", "logs/scalatest.log")

// for additional fancy html reporting using pegdown
testOptions in Test += Tests.Argument("-h", "logs/html")

// dependency graph visualizer setting, usage example 'sbt dependency-tree'
net.virtualvoid.sbt.graph.Plugin.graphSettings
