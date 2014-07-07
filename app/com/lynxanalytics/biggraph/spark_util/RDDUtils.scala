package com.lynxanalytics.biggraph.spark_util

import com.lynxanalytics.biggraph.graph_api._
import com.esotericsoftware.kryo
import org.apache.hadoop
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect._

object RDDUtils {
  val threadLocalKryo = new ThreadLocal[kryo.Kryo] {
    override def initialValue(): kryo.Kryo = {
      val myKryo = new kryo.Kryo()
      myKryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy());
      new BigGraphKryoRegistrator().registerClasses(myKryo)
      myKryo
    }
  }

  def serialize[T](obj: Any): Array[Byte] = {
    val bos = new java.io.ByteArrayOutputStream
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(obj)
    oos.close
    bos.toByteArray
  }

  def kryoDeserialize[T](bytes: Array[Byte]): T = {
    val ois = new kryo.io.Input(bytes)
    threadLocalKryo.get.readClassAndObject(ois).asInstanceOf[T]
  }

  def kryoSerialize[T](obj: Any): Array[Byte] = {
    val bos = new java.io.ByteArrayOutputStream
    val oos = new kryo.io.Output(bos)
    threadLocalKryo.get.writeClassAndObject(oos, obj)
    oos.close
    bos.toByteArray
  }

  implicit class Implicit[T](self: RDD[T]) {
    def numbered: RDD[(Long, T)] = {
      val localCounts = self.glom().map(_.size).collect().scan(0)(_ + _)
      val counts = self.sparkContext.broadcast(localCounts)
      self.mapPartitionsWithIndex((i, p) => {
        (counts.value(i) until counts.value(i + 1)).map(_.toLong).toIterator zip p
      })
    }

    def fastNumbered: RDD[(Long, T)] = {
      self.mapPartitionsWithIndex {
        case (pid, it) => it.zipWithIndex.map {
          case (el, fID) => ((pid.toLong << 32) + fID, el)
        }
      }
    }
  }
}
