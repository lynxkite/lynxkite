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

scalaVersion := "2.11.8"

val sparkVersion = SettingKey[String]("spark-version", "The version of Spark used for building.")

sparkVersion := IO.readLines(baseDirectory.value / "conf/SPARK_VERSION")(0)

libraryDependencies ++= Seq(
  ws, // Play library for making HTTP requests.
  filters, // Play library for compressing HTTP responses.
  // These jackson deps are needed to resolve some jackson version conflict by forcing to use 2.6.5
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  // This one is just for reading YAML.
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.6.5",
  // The below dep is needed to avoid jar version conflict when running in Amazon EMR.
  // (There we use a Hadoop-less Spark build and use Hadoop libs provided by Amazon.
  // This way we get s3 consistent view support.)
  "org.apache.httpcomponents" % "httpclient" % "4.5.1",
  "org.apache.commons" % "commons-lang3" % "3.5",
  "org.apache.commons" % "commons-math3" % "3.4.1",  // Match Spark 2.2.0.
  "org.mindrot" % "jbcrypt" % "0.3m",  // For password hashing.
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  // For accessing S3 fs from local instance.
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3" excludeAll(
    // But we still want to take Hadoop from Spark.
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-common")),
  "net.java.dev.jets3t" % "jets3t" % "0.9.4",
  // Provides HyperLogLogPlus counters. Must be the same version that is
  // used by Spark.
  "com.clearspring.analytics" % "stream" % "2.7.0",
  // JDBC drivers.
  "mysql" % "mysql-connector-java" % "8.0.20",
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
  // Neo4j driver & testing
  "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4",
  "org.neo4j.test" % "neo4j-harness" % "3.5.1" % Test classifier "",
  "org.neo4j" % "neo4j-io" % "3.5.1" % Test classifier "",
  "org.neo4j" % "neo4j-common" % "3.5.1" % Test classifier "",
  "com.sun.jersey" % "jersey-core" % "1.19.4" % Test classifier "", // Required to create Neo4j test server
  "com.lihaoyi" % "ammonite-sshd" % "1.0.3" cross CrossVersion.full excludeAll(
    ExclusionRule(organization="org.specs2", name="specs2_2.11")),
  // Required because of Ammonite using a different scalaz version than the Play framework
  "org.specs2" %% "specs2-junit" % "3.7",
  // For compressed Hive tables.
  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.20",
  // For SPARK-10306.
  "org.scala-lang" % "scala-library" % "2.11.8",
  // Fast linear algebra.
  "org.scalanlp" %% "breeze" % "0.13.1",
  "org.scalanlp" %% "breeze-natives" % "0.13.1",
  // This is a dependency of Spark. Needed here explicitly
  // so that SetupMetricsSingleton compiles.
  "org.eclipse.jetty" % "jetty-servlet" % "8.1.19.v20160209",
  // The Google Cloud Storage connector for Spark and Hive
  "com.google.cloud.bigdataoss" % "gcs-connector" % "1.6.1-hadoop2",
  "org.geotools" % "gt-shapefile" % "20.0",
  "org.geotools" % "gt-epsg-hsql" % "20.0",
  "org.locationtech.jts" % "jts" % "1.16.0",
  // Plot drawing
  "org.vegas-viz" %% "vegas" % "0.3.9",
  "org.vegas-viz" %% "vegas-spark" % "0.3.9",
  // Generate java from proto files. Used by Sphynx.
  "io.grpc" % "grpc-protobuf" % "1.24.0",
  "io.grpc" % "grpc-stub" % "1.24.0",
  "io.grpc" % "grpc-netty" % "1.24.0",
  "com.google.protobuf" % "protobuf-java" % "3.9.2",
  // Used for encrypted connection with Sphynx.
  "io.netty" % "netty-tcnative-boringssl-static" % "2.0.26.Final",
  // This indirect dependency of ours is broken on Maven.
  "javax.media" % "jai_core" % "1.1.3" from "https://repo.osgeo.org/repository/geotools-releases/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
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
  "Twitter Repository" at "https://maven.twttr.com",
  "Geotoolkit.org Repository" at "https://maven.geotoolkit.org",
  "Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/geotools-releases/",
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven")

// Runs "stage", then creates the "stage/version" file.
def myStage = Command.command("stage") { state =>
  import sys.process._
  val res = Project.extract(state).runTask(com.typesafe.sbt.packager.Keys.stage, state)._1
  val date = java.util.Calendar.getInstance.getTime
  val user = util.Properties.userName
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

inConfig(Benchmark)(Defaults.testTasks) ++ Seq(
  testOptions in Benchmark := Seq(Tests.Argument("-n", "Benchmark"))
)

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
  file("sphynx/.build/lynxkite-sphynx") -> "sphynx/lynxkite-sphynx")

sourceDirectory in Assets := new File("web/dist")

mappings in Universal ~= {
  _.filterNot { case (_, relPath) => relPath == "README.md"}
}
