// Supply json play format for type tags

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.spark_util.IDBuckets
import play.api.libs.json
import play.api.libs.json.{ JsArray, JsValue }

import scala.collection.mutable
import scala.reflect.runtime.universe._

object TypeTagToFormat {

  implicit val formatDynamicFormat = json.Json.format[DynamicValue]
  implicit val formatEdge = json.Json.format[Edge]
  implicit val formatUIFilterStatus = json.Json.format[UIFilterStatus]
  implicit val formatUIAttributeAxisOptions = json.Json.format[UIAttributeAxisOptions]
  implicit val formatUIAxisOptions = json.Json.format[UIAxisOptions]
  implicit val formatUIAnimation = json.Json.format[UIAnimation]
  implicit val formatUIAttributeFilter = json.Json.format[UIAttributeFilter]
  implicit val formatUICenterRequest = json.Json.format[UICenterRequest]
  implicit val formatUIStatus = json.Json.format[UIStatus]
  import com.lynxanalytics.biggraph.serving.FrontendJson.fExportToFileResult
  import com.lynxanalytics.biggraph.serving.FrontendJson.fExportToJdbcResult
  import com.lynxanalytics.biggraph.serving.FrontendJson.fDownloadFileRequest
  implicit val formatExportResultMetaData = json.Json.format[ExportResultMetaData]

  implicit object ToJsonFormat extends json.Format[ToJson] {
    def writes(t: ToJson): JsValue = {
      t.toTypedJson
    }
    def reads(j: json.JsValue): json.JsResult[ToJson] = {
      json.JsSuccess(TypedJson.read(j))
    }
  }

  def optionToFormat[T](t: TypeTag[T]): json.Format[Option[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[Option[T]]]
  }

  def pairToFormat[A, B](a: TypeTag[A], b: TypeTag[B]): json.Format[(A, B)] = {
    implicit val innerFormat1 = typeTagToFormat(a)
    implicit val innerFormat2 = typeTagToFormat(b)
    new PairFormat[A, B]
  }

  class PairFormat[A: json.Format, B: json.Format] extends json.Format[(A, B)] {
    def reads(j: json.JsValue): json.JsSuccess[(A, B)] = {
      json.JsSuccess(((j \ "first").as[A], (j \ "second").as[B]))
    }
    def writes(v: (A, B)): json.JsValue = {
      json.Json.obj(
        "first" -> v._1,
        "second" -> v._2
      )
    }
  }

  class MapFormat[A: json.Format, B: json.Format] extends json.Format[Map[A, B]] {
    def reads(jv: json.JsValue): json.JsSuccess[Map[A, B]] = {
      val extracted = jv match {
        case JsArray(arr) => arr.map {
          element => (element \ "key").as[A] -> (element \ "val").as[B]
        }
        case _ => ???
      }
      json.JsSuccess(extracted.toMap)
    }
    def writes(v: Map[A, B]): json.JsValue = {
      val sss = v.map {
        case (key, value) =>
          json.Json.obj(
            "key" -> json.Json.toJson(key),
            "val" -> json.Json.toJson(value)
          )
      }.toSeq
      JsArray(sss)
    }
  }

  class IDBucketsFormat[T: json.Format] extends json.Format[IDBuckets[T]] {
    implicit val mft1 = new MapFormat[T, Long]
    implicit val mft2 = new MapFormat[Long, T]
    def writes(v: IDBuckets[T]): json.JsValue = {
      val sampleOut =
        if (v.sample == null) json.JsNull
        else json.Json.toJson(v.sample.toMap)
      json.Json.obj(
        "counts" -> json.Json.toJson(v.counts.toMap),
        "sample" -> sampleOut
      )
    }
    def reads(j: json.JsValue): json.JsSuccess[IDBuckets[T]] = {
      val immutableCounts = (j \ "counts").as[Map[T, Long]]
      val counts = mutable.Map[T, Long]().withDefaultValue(0) ++ immutableCounts
      val bucket = new IDBuckets[T](counts)
      (j \ "sample") match {
        case JsArray(_) =>
          val immutableSample = (j \ "sample").as[Map[ID, T]]
          bucket.sample = mutable.Map[ID, T]() ++ immutableSample
        case _ => bucket.sample = null
      }
      json.JsSuccess(bucket)
    }
  }

  def bucketsToFormat[T](t: TypeTag[T]): json.Format[IDBuckets[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    new IDBucketsFormat[T]
  }

  def mapToFormat[A, B](a: TypeTag[A], b: TypeTag[B]): json.Format[Map[A, B]] = {
    implicit val innerFormat1 = typeTagToFormat(a)
    implicit val innerFormat2 = typeTagToFormat(b)
    new MapFormat[A, B]
  }

  def seqToFormat[T](t: TypeTag[T]): json.Format[Seq[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[Seq[T]]]
  }

  def setToFormat[T](t: TypeTag[T]): json.Format[Set[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[Set[T]]]
  }

  def indexedSeqToFormat[T](t: TypeTag[T]): json.Format[IndexedSeq[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[IndexedSeq[T]]]
  }

  def listToFormat[T](t: TypeTag[T]): json.Format[List[T]] = {
    implicit val innerFormat = typeTagToFormat(t)
    implicitly[json.Format[List[T]]]
  }

  def typeTagToFormat[T](tag: TypeTag[T]): json.Format[T] = {

    val t = tag.tpe
    val fmt = {
      if (TypeTagUtil.isType[String](t)) implicitly[json.Format[String]]
      else if (TypeTagUtil.isType[Double](t)) implicitly[json.Format[Double]]
      else if (TypeTagUtil.isType[Long](t)) implicitly[json.Format[Long]]
      else if (TypeTagUtil.isType[Boolean](t)) implicitly[json.Format[Boolean]]
      else if (TypeTagUtil.isType[Int](t)) implicitly[json.Format[Int]]
      else if (TypeTagUtil.isType[Float](t)) implicitly[json.Format[Float]]
      else if (TypeTagUtil.isType[DynamicValue](t)) implicitly[json.Format[DynamicValue]]
      else if (TypeTagUtil.isType[UIStatus](t)) implicitly[json.Format[UIStatus]]
      else if (TypeTagUtil.isType[Edge](t)) implicitly[json.Format[Edge]]
      else if (TypeTagUtil.isType[ExportResultMetaData](t)) implicitly[json.Format[ExportResultMetaData]]
      else if (TypeTagUtil.isSubtypeOf[ToJson](t)) ToJsonFormat
      else if (TypeTagUtil.isOfKind1[Option](t)) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        optionToFormat(innerType)
      } else if (TypeTagUtil.isOfKind1[IDBuckets](t)) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        bucketsToFormat(innerType)
      } else if (TypeTagUtil.isOfKind2[Tuple2](t)) {
        val firstInnerType = TypeTagUtil.typeArgs(tag).head
        val secondInnerType = TypeTagUtil.typeArgs(tag).drop(1).head
        pairToFormat(firstInnerType, secondInnerType)
      } else if (TypeTagUtil.isOfKind1[Seq](t)) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        seqToFormat(innerType)
      } else if (TypeTagUtil.isOfKind1[Set](t)) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        setToFormat(innerType)
      } else if (TypeTagUtil.isOfKind1[List](t)) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        listToFormat(innerType)
      } else if (TypeTagUtil.isOfKind1[IndexedSeq](t)) {
        val innerType = TypeTagUtil.typeArgs(tag).head
        indexedSeqToFormat(innerType)
      } else if (TypeTagUtil.isOfKind2[Map](t)) {
        val firstInnerType = TypeTagUtil.typeArgs(tag).head
        val secondInnerType = TypeTagUtil.typeArgs(tag).drop(1).head
        mapToFormat(firstInnerType, secondInnerType)
      } else {
        throw new AssertionError(s"Unsupported type: $t")
      }
    }.asInstanceOf[json.Format[T]]
    fmt
  }
}
