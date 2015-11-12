// Implicit classes that add utility methods to entities. For example edgeBundle.reverse is the
// reversed edge bundle. All the operations have compile-time safety and operate on the metagraph.
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations

object Scripting {

  implicit class RichContainedVertexSet(
    p: EntityContainer[VertexSet])(implicit m: MetaGraphManager) extends RichVertexSet(p.entity)
  implicit class RichVertexSet(self: VertexSet)(implicit manager: MetaGraphManager) {
    def const(value: String): Attribute[String] =
      graph_operations.AddConstantAttribute.run(self, value)
    def const(value: Double): Attribute[Double] =
      graph_operations.AddConstantAttribute.run(self, value)
    def const(value: Int): Attribute[Int] =
      graph_operations.AddConstantAttribute.run(self, value)

    def countScalar: Scalar[Long] = // Named to distinguish from EntityRDDData.count.
      graph_operations.Count.run(self)

    def idAttribute: Attribute[ID] =
      graph_operations.IdAsAttribute.run(self)
  }

  implicit class RichContainedEdgeBundle(
    p: EntityContainer[EdgeBundle])(implicit m: MetaGraphManager) extends RichEdgeBundle(p.entity)
  implicit class RichEdgeBundle(self: EdgeBundle)(implicit manager: MetaGraphManager) {
    def const(value: String): Attribute[String] =
      graph_operations.AddConstantAttribute.run(self.idSet, value)
    def const(value: Double): Attribute[Double] =
      graph_operations.AddConstantAttribute.run(self.idSet, value)
    def const(value: Int): Attribute[Int] =
      graph_operations.AddConstantAttribute.run(self.idSet, value)

    def +(other: EdgeBundle) =
      new BundleChain(Seq(self, other)).getCompositeEdgeBundle._1

    def countScalar: Scalar[Long] = // Named to distinguish from EntityRDDData.count.
      graph_operations.Count.run(self)

    def reverse: EdgeBundle =
      graph_operations.ReverseEdges.run(self)

    def makeSymmetric: EdgeBundle = {
      val op = graph_operations.MakeEdgeBundleSymmetric()
      op(op.es, self).result.symmetric
    }

    def addReversed: EdgeBundle = {
      val op = graph_operations.AddReversedEdges()
      op(op.es, self).result.esPlus
    }
  }

  implicit class RichContainedAttribute[T](
    p: EntityContainer[Attribute[T]])(implicit m: MetaGraphManager)
      extends RichAttribute[T](p.entity)
  implicit class RichAttribute[T](self: Attribute[T])(implicit manager: MetaGraphManager) {
    def countScalar: Scalar[Long] = // Named to distinguish from EntityRDDData.count.
      graph_operations.Count.run(self.vertexSet)

    def pull(function: EdgeBundle) =
      graph_operations.PulledOverVertexAttribute.pullAttributeVia(self, function)

    def fallback(fallback: Attribute[T]): Attribute[T] = {
      val op = graph_operations.AttributeFallback[T]()
      op(op.originalAttr, self)(op.defaultAttr, fallback).result.defaultedAttr
    }

    def asString: Attribute[String] = {
      val op = graph_operations.VertexAttributeToString[T]()
      op(op.attr, self).result.attr
    }

    def join[S](other: Attribute[S]): Attribute[(T, S)] = {
      graph_operations.JoinAttributes.run(self, other)
    }
  }

  implicit class RichContainedStringAttribute(
    p: EntityContainer[Attribute[String]])(implicit m: MetaGraphManager)
      extends RichStringAttribute(p.entity)
  implicit class RichStringAttribute(self: Attribute[String])(implicit manager: MetaGraphManager) {
    def asDouble: Attribute[Double] =
      graph_operations.VertexAttributeToDouble.run(self)
  }

  implicit class RichContainedDoubleAttribute(
    p: EntityContainer[Attribute[Double]])(implicit m: MetaGraphManager)
      extends RichDoubleAttribute(p.entity)
  implicit class RichDoubleAttribute(self: Attribute[Double])(implicit manager: MetaGraphManager) {
    def asLong: Attribute[Long] =
      graph_operations.DoubleAttributeToLong.run(self)
  }

  implicit class RichContainedLongAttribute(
    p: EntityContainer[Attribute[Long]])(implicit m: MetaGraphManager)
      extends RichLongAttribute(p.entity)
  implicit class RichLongAttribute(self: Attribute[Long])(implicit manager: MetaGraphManager) {
    def asDouble: Attribute[Double] =
      graph_operations.LongAttributeToDouble.run(self)
  }

  implicit class RichContainedIntAttribute(
    p: EntityContainer[Attribute[Int]])(implicit m: MetaGraphManager)
      extends RichIntAttribute(p.entity)
  implicit class RichIntAttribute(self: Attribute[Int])(implicit manager: MetaGraphManager) {
    def asLong: Attribute[Long] =
      graph_operations.IntAttributeToLong.run(self)
  }
}
