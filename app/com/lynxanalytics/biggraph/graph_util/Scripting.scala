// Implicit classes that add utility methods to entities. For example edgeBundle.reverse is the
// reversed edge bundle. All the operations have compile-time safety and operate on the metagraph.
package com.lynxanalytics.biggraph.graph_util

import scala.reflect.runtime.universe.TypeTag

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

    // A random attribute with a standard normal distribution.
    def randomAttribute(seed: Int): Attribute[Double] = {
      val op = graph_operations.AddRandomAttribute(seed, "Standard Normal")
      op(op.vs, self).result.attr
    }

    def loops: EdgeBundle = {
      val op = graph_operations.LoopEdgeBundle()
      op(op.vs, self).result.eb
    }

    def emptyEdgeBundle: EdgeBundle = {
      val op = graph_operations.EmptyEdgeBundle()
      op(op.src, self)(op.dst, self).result.eb
    }
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

    def concat(other: EdgeBundle) =
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

    def pullVia(function: EdgeBundle) =
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

    def deriveX[S: TypeTag](expression: String): Attribute[S] = {
      graph_operations.DeriveJS.deriveFromAttributes[S](
        expression, Seq("x" -> self), self.vertexSet)
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
    def asDouble: Attribute[Double] =
      graph_operations.IntAttributeToDouble.run(self)
  }

  implicit class RichContainedTable(
    p: EntityContainer[Table])(implicit m: MetaGraphManager)
      extends RichTable(p.entity)
  implicit class RichTable(self: Table)(implicit manager: MetaGraphManager) {
    def toAttributes =
      graph_operations.TableToAttributes.run(self)
  }

  // Take the union of edge bundles that are parallel, that is that are going between the same
  // two vertex sets.
  def parallelEdgeBundleUnion(
    first: EdgeBundle, others: EdgeBundle*)(
      implicit m: MetaGraphManager): EdgeBundle = {
    if (others.isEmpty) first
    else {
      others.foreach { other =>
        assert(
          first.srcVertexSet.gUID == other.srcVertexSet.gUID,
          s"Source vertex set of $first does not match that of $other so" +
            " they cannot be used together in a parallelEdgeBundleUnion")
        assert(first.dstVertexSet.gUID == other.dstVertexSet.gUID,
          s"Destination vertex set of $first does not match that of $other so" +
            " they cannot be used together in a parallelEdgeBundleUnion")
      }
      val all = first +: others
      val idSetUnion = {
        val op = graph_operations.VertexSetUnion(all.size)
        op(op.vss, all.map(_.idSet)).result
      }
      val ebUnion = {
        val op = graph_operations.EdgeBundleUnion(all.size)
        op(op.ebs, all)(op.injections, idSetUnion.injections.map(_.entity)).result
      }
      ebUnion.union
    }
  }

  // Take the union of edge bundles that potentially go between different vertex set pairs.
  // We first take the union of all different source vertex sets and the union of all
  // different destination vertex sets,
  // induce all the bundles to go between these two new vertex sets and then take their union.
  // In effect, this corresponds to the mathematical "model" where we assume all different
  // vertex sets represent disjoint mathematical sets.
  def generalEdgeBundleUnion(
    first: EdgeBundle, others: EdgeBundle*)(
      implicit m: MetaGraphManager): EdgeBundle = {
    if (others.isEmpty) first
    else {
      val all = first +: others
      // We sort these to make sure that the we get the same union vertex set if we start from
      // the same component vertex sets. (E.g. A U B is the same as B U A.)
      val srcs = all.map(_.srcVertexSet).toSet.toSeq.sortBy[java.util.UUID](_.gUID)
      val dsts = all.map(_.dstVertexSet).toSet.toSeq.sortBy[java.util.UUID](_.gUID)

      val induceSrc = (srcs.size > 1)
      val induceDst = (dsts.size > 1)

      val inducedAll = if (induceSrc || induceDst) {
        var opBuilders = all.map { eb =>
          val op = graph_operations.InducedEdgeBundle(induceSrc, induceDst)
          op(op.edges, eb)
        }
        if (induceSrc) {
          val op = graph_operations.VertexSetUnion(srcs.size)
          val injections = op(op.vss, srcs).result.injections
          val injectionMap = (srcs zip injections).toMap
          opBuilders = (opBuilders zip all).map {
            case (builder, eb) =>
              builder(builder.op.srcMapping, injectionMap(eb.srcVertexSet))
          }
        }
        if (induceDst) {
          val op = graph_operations.VertexSetUnion(dsts.size)
          val injections = op(op.vss, dsts).result.injections
          val injectionMap = (dsts zip injections).toMap
          opBuilders = (opBuilders zip all).map {
            case (builder, eb) =>
              builder(builder.op.dstMapping, injectionMap(eb.dstVertexSet))
          }
        }
        opBuilders.map(_.result.induced.entity)
      } else {
        all
      }
      parallelEdgeBundleUnion(inducedAll.head, inducedAll.tail: _*)
    }
  }
}
