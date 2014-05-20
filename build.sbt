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
  "org.apache.commons" % "commons-lang3" % "3.3",
  "org.apache.spark" %% "spark-core" % "0.9.1",
  "org.apache.spark" %% "spark-graphx" % "0.9.1",
  "org.scalatest" %% "scalatest" % "2.1.5" % "test")

play.Project.playScalaSettings

// org.scalatest does not take testOptions.
testOptions in Test := Nil
