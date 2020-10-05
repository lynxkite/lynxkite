// Comment to get more information during initialization
logLevel := Level.Warn

// The Typesafe repository
resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.3.7")

// Code formatting.
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.0")

// Package dependencies. Run "dependencyList" or "dependencyGraph" to access the data.
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

// Adds the "dumpLicenseReport" command.
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.2.0")
