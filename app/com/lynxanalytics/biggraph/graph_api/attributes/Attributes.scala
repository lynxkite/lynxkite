package com.lynxanalytics.biggraph.graph_api.attributes

import scala.collection.immutable
import scala.reflect.runtime.universe._

/**
 * Represents an index that can be used to access elements of
 * a DenseAttributes object.
 *
 * DO NOT use any of the internals of this class in client code!!
 * TODO(xandrew): find some way to hide class internals.
 */
class AttributeIndex[T](val idx: Int) extends AnyVal

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
    val attributeSeq: immutable.Seq[String],
    nextIdx: Int) {
  def getReadIndexFor[T](name: String)(implicit c: TypeTag[T]): AttributeReadIndex[T] = {
    attributes(name).forClassRead(c)
  }

  def isReadableAs[T](name: String)(implicit c: TypeTag[T]): Boolean = {
    attributes.contains(name) && attributes(name).readableAs(c)
  }

  def getWriteIndexFor[T](name: String)(implicit c: TypeTag[T]): AttributeWriteIndex[T] = {
    attributes(name).forClassWrite(c)
  }

  def isWritableAs[T](name: String)(implicit c: TypeTag[T]): Boolean = {
    attributes.contains(name) && attributes(name).writableAs(c)
  }

  def addAttribute[T](name: String)(implicit c: TypeTag[T]): SignatureExtension = {
    SignatureExtension(
      new AttributeSignature(attributes +
                               (name -> TypedAttributeIndex[T](
                                  nextIdx, c)),
                             attributeSeq :+ name,
                             nextIdx + 1),
      PrimitiveCloner(1))
  }

  def getAttributesReadableAs[T](implicit c: TypeTag[T]): Seq[String] = {
    attributes.flatMap({
      case (name, tidx) => if (tidx.readableAs(c)) Some(name) else None
    }).toSeq
  }

  /**
   * Returns all attributes of a given type with their values.
   * Convenience method, this cannot be used on workers as signatures are not
   * serializable, so normally they are not available in closures.
   * Also first getting the indexes and then using those to get value from the
   * individual DenseAttributes objects is faster.
   */
  def getAttributeValues[T](attr: DenseAttributes)(implicit c: TypeTag[T])
      : Map[String, T] = {
    getAttributesReadableAs[T]
      .map(name => (name, attr(getReadIndexFor[T](name)))).toMap
  }

  override lazy val toString: String = {
    "AttributeSignature: %s".format(
      attributeSeq.map(name => (name, attributes(name).toString)))
  }

  val size = nextIdx + 1

  private case class PrimitiveCloner(numNewAttributes: Int) extends ExtensionCloner {
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
}
object AttributeSignature {
  def empty = new AttributeSignature(Map(), immutable.Seq(), 0)
}

/*
 * Clones a DenseAttributes to one with a larger signature.
 *
 * This class takes a DenseAttributes object corresponding to a narrower signature and cretes
 * one corresponding to a larger, derived signature. Cloners can be obtained when building new
 * signatures from old ones by adding attributes.
 */
abstract class ExtensionCloner extends Serializable {
  def clone(original: DenseAttributes): DenseAttributes
  def composeWith(nextCloner: ExtensionCloner): ExtensionCloner
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
class DenseAttributes private (data: Array[Any])
    extends Serializable {
  def apply[T](idx: AttributeIndex[T]): T = {
    return data(idx.idx).asInstanceOf[T]
  }

  def set[T](idx: AttributeIndex[T], value: T): DenseAttributes = {
    data(idx.idx) = value
    return this
  }

  override def clone(): DenseAttributes = {
    new DenseAttributes(data.clone)
  }

  private[attributes] def cloneWithAdditionalAttributes(numAdditionalAttributes: Int) = {
    new DenseAttributes(Array.concat(
      data, Array.fill[Any](numAdditionalAttributes)(null)))
  }
}
object DenseAttributes {
  def apply(sig: AttributeSignature): DenseAttributes =
    new DenseAttributes(Array.fill[Any](sig.size)(null))
}
