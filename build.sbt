import com.typesafe.sbt.packager.Keys.bashScriptExtraDefines

name := "biggraph"

javaOptions in Test := Seq(
  "-Dsun.io.serialization.extendedDebugInfo=true",
  "-Dbiggraph.default.partitions.per.core=1")

scalacOptions ++= Seq("-feature", "-deprecation")

version := "0.1-SNAPSHOT"

sources in doc in Compile := List()  // Disable doc generation.

publishArtifact in packageSrc := false  // Don't package source.

scalaVersion := "2.10.4"

val sparkVersion = SettingKey[String]("spark-version", "The version of Spark used for building.")

sparkVersion := IO.readLines(baseDirectory.value / "conf/SPARK_VERSION")(0)

libraryDependencies ++= Seq(
  anorm, // Play library for making SQL queries.
  ws, // Play library for making HTTP requests.
  "org.apache.commons" % "commons-lang3" % "3.3",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided" excludeAll(
    // Version disagreements with Play.
    ExclusionRule(organization = "org.slf4j", name = "slf4j-api")),
  "org.mindrot" % "jbcrypt" % "0.3m",  // For password hashing.
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value % "provided",
  // JDBC drivers.
  "mysql" % "mysql-connector-java" % "5.1.34",
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.xerial" % "sqlite-jdbc" % "3.8.7")

// Runs "stage", then creates the "stage/version" file.
def myStage = Command.command("stage") { state =>
  import sys.process._
  val res = Project.extract(state).runTask(com.typesafe.sbt.packager.Keys.stage, state)._1
  val date = java.util.Calendar.getInstance.getTime
  val user = util.Properties.userName
  val branch = "git rev-parse --abbrev-ref HEAD".!!
  val modified = if ("git status --porcelain".!!.nonEmpty) "modified" else "mint"
  val lastCommit = "git log -1 --oneline".!!
  IO.write(new java.io.File("stage/version"), s"Staged at $date by $user from $modified $branch (at $lastCommit)\n")
  res
}

commands += myStage

// Save logs to a file. Do not run benchmarks by default. (Use "sbt bench:test" to run them.)
testOptions in Test := Seq(Tests.Argument("-fWDF", "logs/sbttest.out", "-l", "Benchmark"))

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(scalariform.formatter.preferences.AlignParameters, true)
  .setPreference(scalariform.formatter.preferences.DoubleIndentClassDeclaration, true)

// Separate config for benchmarks.
lazy val Benchmark = (config("bench") extend Test)

inConfig(Benchmark)(Defaults.testTasks) ++ Seq(
  testOptions in Benchmark := Seq(Tests.Argument("-n", "Benchmark"))
)

lazy val root = project.in(file("."))
  .enablePlugins(PlayScala)
  .configs(Benchmark)

bashScriptExtraDefines ++= IO.readLines(baseDirectory.value / "tools" / "call_spark_submit.sh")
