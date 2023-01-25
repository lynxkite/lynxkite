// Comment to get more information during initialization
logLevel := Level.Warn
ThisBuild / evictionErrorLevel := Level.Info

// The Typesafe repository
resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.8.19")

// Code formatting.
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

// Package dependencies. Run "dependencyList" or "dependencyGraph" to access the data.
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

// Adds the "dumpLicenseReport" command.
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.2.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")
