// Kryo registration and the creation of the SparkContext.
package com.lynxanalytics.biggraph.spark_util

import com.esotericsoftware.kryo.Kryo
import com.google.cloud.hadoop.fs.gcs
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoRegistrator
import scala.collection.immutable
import scala.collection.mutable
import scala.reflect.ClassTag

import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.spark_util

private object SparkStageJars {
  val classesToBundle: Seq[Class[_]] = Seq(
    getClass(),
    classOf[com.mysql.jdbc.Driver],
    classOf[org.postgresql.Driver],
    classOf[org.sqlite.JDBC],
    classOf[gcs.GoogleHadoopFileSystem],
    classOf[play.api.libs.json.JsValue])
  val extraJarsToBundle =
    scala.util.Properties.envOrElse("KITE_EXTRA_JARS", "")
      .split(":", -1)
      .map(_.trim)
      .filter(_ != "")
  val jars =
    classesToBundle.map(_.getProtectionDomain().getCodeSource().getLocation().getPath()) ++
      extraJarsToBundle
  require(
    jars.forall(_.endsWith(".jar")),
    "You need to run this from a jar. Use 'sbt stage' to get one.")
}

// Placeholders for deleted classes.
class DeadClass1
class DeadClass2
class DeadClass3

class BigGraphKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    // Adding one more line? Do it at the bottom!
    // Deleting a line? Do not.
    // Types will change IDs otherwise.
    kryo.setRegistrationRequired(true)
    kryo.register(classOf[scala.Tuple2[_, _]])
    kryo.register(classOf[Array[Any]])
    kryo.register(classOf[mutable.WrappedArray$ofRef])
    kryo.register(classOf[mutable.ArrayBuffer[_]])
    kryo.register(classOf[Array[mutable.ArrayBuffer[_]]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Long]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Tuple2[_, _]]])
    kryo.register(classOf[Array[Tuple3[_, _, _]]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[scala.runtime.BoxedUnit])
    kryo.register(classOf[graph_api.CompactUndirectedGraph])
    kryo.register(classOf[::[_]])
    kryo.register(Nil.getClass)
    kryo.register(None.getClass)
    kryo.register(Set.empty[Int].getClass)
    kryo.register(classOf[mutable.ArrayBuffer[Any]])
    kryo.register(classOf[graph_api.Edge])
    kryo.register(classOf[Array[Seq[_]]])
    kryo.register(classOf[Array[graph_api.Edge]])
    kryo.register((0L, 0.0).getClass)
    kryo.register(Class.forName("org.apache.spark.util.BoundedPriorityQueue")) // SPARK-2306
    kryo.register(classOf[graph_operations.ComputeTopValues.PairOrdering[_]])
    kryo.register(classOf[collection.immutable.Range])
    kryo.register(classOf[DeadClass1])
    kryo.register(classOf[Array[DeadClass1]])
    kryo.register(classOf[mutable.WrappedArray$ofInt])
    kryo.register(('x', 'x').getClass)
    kryo.register(classOf[collection.mutable.Map[_, _]])
    kryo.register(classOf[scala.Tuple2[Double, Double]])
    kryo.register(classOf[Array[Tuple2[Long, Int]]])
    kryo.register(classOf[Option[_]])
    kryo.register(classOf[Array[Option[_]]])
    kryo.register(classOf[Vector[_]])
    kryo.register(classOf[graph_operations.DynamicValue])
    kryo.register(classOf[Array[graph_operations.DynamicValue]])
    kryo.register(ClassTag(Class.forName("org.apache.spark.util.collection.CompactBuffer")).wrap.runtimeClass)
    kryo.register(classOf[collection.mutable.Map$WithDefault])
    kryo.register(classOf[collection.mutable.Map$$anonfun$withDefaultValue$1])
    kryo.register(classOf[spark_util.IDBuckets[_]])
    kryo.register(classOf[graph_operations.Stats])
    kryo.register(classOf[Array[graph_operations.Stats]])
    kryo.register((0L, 0).getClass)
    kryo.register(classOf[Array[org.apache.spark.mllib.linalg.Vector]])
    kryo.register(classOf[org.apache.spark.mllib.linalg.DenseVector])
    kryo.register(breeze.linalg.DenseVector(Array[Double](0)).getClass)
    kryo.register(classOf[DeadClass2])
    kryo.register(classOf[DeadClass3])
    kryo.register((0.0, 0.0).getClass)
    kryo.register(math.Numeric.LongIsIntegral.getClass) // For using NumericRanges with sc.parallelize.
    // The next three are required by some operations after the Spark 1.3.0 updgrade. (SPARK-6497)
    kryo.register(classOf[scala.reflect.ManifestFactory$$anon$10])
    kryo.register(classOf[scala.reflect.ClassTag$$anon$1])
    kryo.register(classOf[Class[_]])
    // Add new stuff just above this line! Thanks.
    // Adding Foo$mcXXX$sp? It is a type specialization. Register the decoded type instead!
    // Z = Boolean, B = Byte, C = Char, D = Double, F = Float, I = Int, J = Long, S = Short.
  }
}

class BigGraphKryoRegistratorWithDebug extends BigGraphKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    import com.esotericsoftware.minlog.Log
    import com.esotericsoftware.minlog.Log._
    Log.set(LEVEL_TRACE);

    super.registerClasses(kryo)
  }
}

object BigGraphSparkContext {
  def createKryo(): Kryo = {
    val myKryo = new Kryo()
    myKryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy());
    new BigGraphKryoRegistrator().registerClasses(myKryo)
    myKryo
  }
  def apply(
    appName: String,
    useKryo: Boolean = true,
    debugKryo: Boolean = false,
    useJars: Boolean = true,
    master: String = ""): SparkContext = {
    val versionFound = org.apache.spark.SPARK_VERSION
    val versionRequired = scala.io.Source.fromURL(getClass.getResource("/SPARK_VERSION")).mkString.trim
    assert(versionFound == versionRequired,
      s"Needs Apache Spark version $versionRequired. Found $versionFound.")
    var sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.executor.memory",
        scala.util.Properties.envOrElse("EXECUTOR_MEMORY", "1700m"))
      .set("spark.akka.threads",
        scala.util.Properties.envOrElse("AKKA_THREADS", "4")) // set it to number of cores on master
      .set("spark.local.dir", scala.util.Properties.envOrElse("SPARK_DIR", "/tmp"))
      // Speculative execution will start extra copies of tasks to eliminate long tail latency.
      .set("spark.speculation", "true") // Enable speculative execution.
      .set("spark.speculation.interval", "1000") // (Milliseconds.) How often to check.
      .set("spark.speculation.quantile", "0.90") // (Fraction.) This much of the stage has to complete first.
      .set("spark.speculation.multiplier", "2") // (Ratio.) Task has to be this much slower than the median.
      .set(
        // Enables fair scheduling, that is tasks of all running jobs are scheduled round-robin
        // instead of one job finishes completely first. See:
        // http://spark.apache.org/docs/latest/job-scheduling.html
        "spark.scheduler.mode",
        "FAIR")
      .set("spark.core.connection.ack.wait.timeout", "240")
      .set(
        "spark.scheduler.allocation.file",
        scala.util.Properties.envOrElse("KITE_SCHEDULER_POOLS_CONFIG", "conf/scheduler-pools.xml"))
    if (useKryo) {
      sparkConf = sparkConf
        .set(
          "spark.serializer",
          "org.apache.spark.serializer.KryoSerializer")
        .set(
          "spark.kryo.registrator",
          if (debugKryo) "com.lynxanalytics.biggraph.spark_util.BigGraphKryoRegistratorWithDebug"
          else "com.lynxanalytics.biggraph.spark_util.BigGraphKryoRegistrator")
    }
    if (useJars) {
      sparkConf = sparkConf.setJars(SparkStageJars.jars)
    }
    if (master != "") {
      sparkConf = sparkConf.setMaster(master)
    }
    return new SparkContext(sparkConf)
  }
}
