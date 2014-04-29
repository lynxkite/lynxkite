name := "biggraph"

version := "0.1-SNAPSHOT"

sources in doc in Compile := List()  // Disable doc generation.

publishArtifact in packageSrc := false  // Don't package source.

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "0.9.0-incubating",
  "org.apache.spark" %% "spark-graphx" % "0.9.0-incubating",
  "org.scalatest" %% "scalatest" % "2.1.0" % "test")

play.Project.playScalaSettings
