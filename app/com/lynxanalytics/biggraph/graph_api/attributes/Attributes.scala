package com.lynxanalytics.biggraph.graph_api.attributes

import scala.collection.immutable
import scala.reflect.runtime.universe._

/**
 * Represents an index that can be used to access elements of
 * a DenseAttributes object.
 */
class AttributeIndex[T](private[attributes] val idx: Int) extends Serializable

/**
 * Class representing what attributes are available on some entity.
 *
 * It defines the names and types of available attributes and an ordering
 * on the attributes.
 *
 * This class indirectly uses reflection, so it is not thread-safe (see
 * http://docs.scala-lang.org/overviews/reflection/thread-safety.html). Do not
 * use it on Spark workers.
 */
class AttributeSignature private (
    attributes: Map[String, TypedAttributeIndex[_]],
    val attributeSeq: immutable.Seq[String]) {
  def readIndex[T](name: String)(implicit c: TypeTag[T]): AttributeReadIndex[T] = {
    attributes(name).forClassRead(c)
  }

  def canRead[T](name: String)(implicit c: TypeTag[T]): Boolean = {
    attributes.contains(name) && attributes(name).readableAs(c)
  }

  def writeIndex[T](name: String)(implicit c: TypeTag[T]): AttributeWriteIndex[T] = {
    attributes(name).forClassWrite(c)
  }

  def canWrite[T](name: String)(implicit c: TypeTag[T]): Boolean = {
    attributes.contains(name) && attributes(name).writableAs(c)
  }

  def addAttribute[T](name: String)(implicit c: TypeTag[T]): SignatureExtension = {
    SignatureExtension(
      new AttributeSignature(attributes +
                               (name -> TypedAttributeIndex[T](
                                  attributeSeq.size, c)),
                             attributeSeq :+ name),
      PrimitiveCloner(1))
  }

  def getAttributesReadableAs[T](implicit c: TypeTag[T]): Seq[String] = {
    attributes.flatMap({
      case (name, tidx) => if (tidx.readableAs(c)) Some(name) else None
    }).toSeq
  }

  def maker: DenseAttributesMaker = new PrimitiveMaker(size)

  override lazy val toString: String = {
    "AttributeSignature: %s".format(
      attributeSeq.map(name => (name, attributes(name).toString)))
  }

  val size = attributeSeq.size
}
object AttributeSignature {
  def empty = new AttributeSignature(Map(), immutable.Seq())
}

/*
 * Clones a DenseAttributes to one with a larger signature.
 *
 * This class takes a DenseAttributes object corresponding to a narrower signature and creates
 * one corresponding to a larger, derived signature. Cloners can be obtained when building new
 * signatures from old ones by adding attributes.
 */
trait ExtensionCloner extends Serializable {
  def clone(original: DenseAttributes): DenseAttributes
  def composeWith(nextCloner: ExtensionCloner): ExtensionCloner
}

/*
 * One should use instance of this class to create new DenseAttributes objects.
 *
 * Get your maker from your signature.
 */
trait DenseAttributesMaker extends Serializable {
  def make(): DenseAttributes
}

/*
 * Couples an AttributeSignature with an ExtensionCloner.
 *
 * This is used to conveniently create new signatures from old
 * ones together with the cloner using the builder pattern.
 */
case class SignatureExtension(signature: AttributeSignature,
                              cloner: ExtensionCloner) {
  def addAttribute[T](name: String)(implicit c: TypeTag[T]): SignatureExtension = {
    val oneStepExtension = signature.addAttribute[T](name)
    SignatureExtension(oneStepExtension.signature, cloner.composeWith(oneStepExtension.cloner))
  }
}


/*
 * Class used to represent raw attribute data on entities (e.g. nodes, edges).
 *
 * This class doesn't know anything about what data it stores. One needs to use it together
 * with indices obtained from the corresponding AttributeSignature to make sense of the data.
 */
class DenseAttributes private[attributes] (data: Array[Any])
    extends Serializable {
  def apply[T](idx: AttributeIndex[T]): T = {
    return data(idx.idx).asInstanceOf[T]
  }

  def set[T](idx: AttributeIndex[T], value: T): DenseAttributes = {
    data(idx.idx) = value
    return this
  }

  private[attributes] def cloneWithAdditionalAttributes(numAdditionalAttributes: Int) = {
    new DenseAttributes(Array.concat(
      data, Array.fill[Any](numAdditionalAttributes)(null)))
  }
}

private[attributes] case class PrimitiveCloner(numNewAttributes: Int) extends ExtensionCloner {
  def clone(original: DenseAttributes): DenseAttributes = {
    original.cloneWithAdditionalAttributes(numNewAttributes)
  }
  def composeWith(nextCloner: ExtensionCloner): ExtensionCloner = {
    nextCloner match {
      case PrimitiveCloner(otherNumNewAttributes)
          => PrimitiveCloner(numNewAttributes + otherNumNewAttributes)
    }
  }
}

private[attributes] class PrimitiveMaker(size: Int) extends DenseAttributesMaker {
  def make(): DenseAttributes = {
    new DenseAttributes(Array.fill[Any](size)(null))
  }
}
