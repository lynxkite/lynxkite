package com.lynxanalytics.biggraph.spark_util

import com.esotericsoftware.kryo.Kryo
import com.google.cloud.hadoop.fs.gcs
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx
import org.apache.spark.serializer.KryoRegistrator
import scala.collection.immutable
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.graph_api.attributes

private object SparkStageJars {
  val classesToBundle: Seq[Class[_]] = Seq(
    getClass(),
    classOf[gcs.GoogleHadoopFileSystem])
  val jars = classesToBundle.map(_.getProtectionDomain().getCodeSource().getLocation().getPath())
  require(
    jars.forall(_.endsWith(".jar")),
    "You need to run this from a jar. Use 'sbt stage' to get one.")
}

class BigGraphKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    // Adding one more line? Do it at the bottom!
    // Types will change IDs otherwise.
    kryo.setRegistrationRequired(true)
    kryo.register(classOf[graphx.Edge[_]])
    kryo.register(classOf[scala.Tuple2[_, _]])
    kryo.register(classOf[graphx.EdgeTriplet[_, _]])
    kryo.register(classOf[Array[graphx.Edge[_]]])
    kryo.register(classOf[Array[graphx.EdgeTriplet[_, _]]])
    kryo.register(classOf[attributes.DenseAttributes])
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
    // Set.EmptySet[_] is private.
    kryo.register(Set.empty[Int].getClass)
    kryo.register(classOf[mutable.ArrayBuffer[Any]])
    kryo.register(classOf[graph_api.Edge])
    kryo.register(classOf[Array[Seq[_]]])
    kryo.register(classOf[Array[graph_api.Edge]])
    kryo.register((0L, 0.0).getClass);
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
  def apply(
    appName: String,
    masterURL: String,
    useKryo: Boolean = true,
    debugKryo: Boolean = false,
    useJars: Boolean = true): SparkContext = {
    var sparkConf = new SparkConf()
      .setMaster(masterURL)
      .setAppName(appName)
      .set("spark.executor.memory",
        scala.util.Properties.envOrElse("EXECUTOR_MEMORY", "1700m"))
      .set("spark.kryoserializer.buffer.mb", "256")
      .set("spark.akka.threads",
        scala.util.Properties.envOrElse("AKKA_THREADS", "4")) // set it to number of cores on master
      .set("spark.local.dir", scala.util.Properties.envOrElse("SPARK_DIR", "/tmp"))
      .set("spark.speculation", "true")
      .set("spark.speculation.interval", "1000")
      .set("spark.speculation.quantile", "0.90")
      .set("spark.speculation.multiplier", "2")
    if (useKryo) {
      sparkConf = sparkConf
        .set("spark.serializer",
          "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator",
          if (debugKryo) "com.lynxanalytics.biggraph.spark_util.BigGraphKryoRegistratorWithDebug"
          else "com.lynxanalytics.biggraph.spark_util.BigGraphKryoRegistrator")
    }
    if (useJars) {
      sparkConf = sparkConf.setJars(SparkStageJars.jars)
    }
    return new SparkContext(sparkConf)
  }
}
