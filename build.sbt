import com.typesafe.sbt.packager.Keys.bashScriptExtraDefines

name := "lynxkite"

javaOptions in Test := Seq(
  "-Dlynxkite.default.partitions.per.core=1",
  "-Djava.security.policy=conf/security.policy",
  "-XX:PermSize=256M")

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  // Restrict file name length to support encrypted file systems.
  "-Xmax-classfile-name", "120",
  // TODO: Suppress warnings as necessary and enable checks.
  // "-Ywarn-dead-code",
  // "-Ywarn-unused",
  // "-Ywarn-unused-import",
  // "-Xlint:_,-adapted-args,-type-parameter-shadow,-inaccessible",
  "-Xfatal-warnings")

version := "0.1-SNAPSHOT"

sources in doc in Compile := List()  // Disable doc generation.

publishArtifact in packageSrc := false  // Don't package source.

scalaVersion := "2.12.13"

val sparkVersion = SettingKey[String]("spark-version", "The version of Spark used for building.")

sparkVersion := IO.readLines(baseDirectory.value / "conf/SPARK_VERSION")(0)

libraryDependencies ++= Seq(
  guice, // Dependency injection for Play.
  filters, // Play library for compressing HTTP responses.
  // Play and Spark depend on different Netty versions. We help them decide here.
  "io.netty" % "netty-all" % "4.1.51.Final",
  "org.mindrot" % "jbcrypt" % "0.3m",  // For password hashing.
  "org.scalatest" %% "scalatest" % "3.2.7" % "test",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  // For accessing S3 fs from local instance.
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3" excludeAll(
    // But we still want to take Hadoop from Spark.
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-common")),
  "net.java.dev.jets3t" % "jets3t" % "0.9.4",
  // Provides HyperLogLogPlus counters. Must be the same version that is
  // used by Spark.
  "com.clearspring.analytics" % "stream" % "2.9.6",
  // JDBC drivers.
  "mysql" % "mysql-connector-java" % "8.0.20",
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
  "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.6",
  // The Google Cloud Storage connector for Spark and Hive
  "com.google.cloud.bigdataoss" % "gcs-connector" % "1.6.1-hadoop2",
  "org.geotools" % "gt-shapefile" % "20.0",
  "org.geotools" % "gt-epsg-hsql" % "20.0",
  "org.locationtech.jts" % "jts" % "1.16.0",
  // Generate java from proto files. Used by Sphynx.
  "io.grpc" % "grpc-protobuf" % "1.24.0",
  "io.grpc" % "grpc-stub" % "1.24.0",
  "io.grpc" % "grpc-netty" % "1.24.0",
  "com.google.protobuf" % "protobuf-java" % "3.9.2",
  // Used for encrypted connection with Sphynx.
  "io.netty" % "netty-tcnative-boringssl-static" % "2.0.26.Final",
  // This indirect dependency of ours is broken on Maven.
  "javax.media" % "jai_core" % "1.1.3" from "https://repo.osgeo.org/repository/geotools-releases/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar",
  // Used for working with AVRO files. 
  "org.apache.spark" %% "spark-avro" % sparkVersion.value,
  // For Neo4j import/export. Doesn't work, see https://github.com/neo4j-contrib/neo4j-spark-connector/issues/339.
  //"neo4j-contrib" %% "neo4j-connector-apache-spark" % "4.0.1_for_spark_3",
  // This can be removed when the line above works.
  "org.neo4j.driver" % "neo4j-java-driver" % "4.2.5",
  // For Neo4j tests.
  "org.testcontainers" % "testcontainers" % "1.15.2" % Test,
  "org.testcontainers" % "neo4j" % "1.15.2" % Test,
  // Used for working with Delta tables.
  "io.delta" %% "delta-core" % "0.6.1",
  // All kinds of parsing, e.g. filters.
  "com.lihaoyi" %% "fastparse" % "1.0.0"
)

// We put the local Spark installation on the classpath for compilation and testing instead of using
// it from Maven. The version on Maven pulls in an unpredictable (old) version of Hadoop.
def sparkJars(version: String) = {
  val home = System.getenv("HOME")
  val jarsDir = new java.io.File(s"$home/spark/spark-$version/jars")
  (jarsDir * "*.jar").get
}

dependencyClasspath in Compile ++= sparkJars(sparkVersion.value)

dependencyClasspath in Test ++= sparkJars(sparkVersion.value)

resolvers ++= Seq(
  "Geotoolkit.org Repository" at "https://maven.geotoolkit.org",
  "Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/geotools-releases/",
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven")

// Runs "stage", then creates the "stage/version" file.
def myStage = Command.command("stage") { state =>
  import sys.process._
  val res = Project.extract(state).runTask(com.typesafe.sbt.packager.Keys.stage, state)._1
  val date = java.util.Calendar.getInstance.getTime
  val user = System.getProperty("user.name")
  val branch = "git rev-parse --abbrev-ref HEAD".!!
  val modified = if ("git status --porcelain".!!.nonEmpty) "modified" else "mint"
  val lastCommit = "git log -1 --oneline".!!
  IO.write(new java.io.File("target/universal/stage/version"), s"Staged at $date by $user from $modified $branch (at $lastCommit)\n")
  res
}

commands += myStage

// Save logs to a file. Do not run benchmarks by default. (Use "sbt bench:test" to run them.)
testOptions in Test := Seq(Tests.Argument("-oDF", "-fWDF", "logs/sbttest.out", "-l", "Benchmark"))

// Separate config for benchmarks.
lazy val Benchmark = (config("bench") extend Test)

testOptions in Benchmark := Seq(Tests.Argument("-n", "Benchmark"))
Global / excludeLintKeys += testOptions in Benchmark

lazy val root = project.in(file("."))
  .enablePlugins(PlayScala)
  .configs(Benchmark)

bashScriptExtraDefines ++= IO.readLines(baseDirectory.value / "tools" / "call_spark_submit.sh")

def dirContents(baseDir: File, dirs: String*) = {
  val subDir = dirs.foldLeft(baseDir) { (file, dir) => file / dir}
  val pathFinder = subDir * "*"
  pathFinder.get map {
    tool: File =>
      tool -> (dirs.mkString("/") + "/" + tool.getName)
  }
}

mappings in Universal ++= dirContents(baseDirectory.value, "tools", "monitoring")
mappings in Universal ++= dirContents(baseDirectory.value, "tools", "monitoring", "dashboards")
mappings in Universal ++= dirContents(baseDirectory.value, "tools", "graphray")
mappings in Universal ++= dirContents(baseDirectory.value, "built-ins")
mappings in Universal ++= dirContents(baseDirectory.value, "sphynx", "python")
mappings in Universal ++= Seq(
  file("tools/rmoperation.py") -> "tools/rmoperation.py",
  file("tools/kite_meta_hdfs_backup.sh") -> "tools/kite_meta_hdfs_backup.sh",
  file("tools/install_spark.sh") -> "tools/install_spark.sh",
  file("sphynx/.build/lynxkite-sphynx") -> "sphynx/lynxkite-sphynx",
  file("sphynx/.build/libnetworkit.so") -> "sphynx/libnetworkit.so")

sourceDirectory in Assets := new File("web/dist")

mappings in Universal ~= {
  _.filterNot { case (_, relPath) => relPath == "README.md"}
}
