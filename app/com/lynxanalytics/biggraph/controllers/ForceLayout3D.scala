package com.lynxanalytics.biggraph.controllers

import scala.collection.mutable

// Generates 3D positions for FEEdge endpoints.
object ForceLayout3D {
  case class Vertex(id: Int, mass: Double, var pos: FE3DPosition = FE3DPosition(0.0, 0.0, 0.0))

  final val IdealDistance = 10.0
  final val Fraction = 0.01
  final val Iterations = 50

  def apply(edges: Seq[FEEdge]): Seq[FEEdge] = {
    val edgeWeights = edges.map(e => e.a -> e.size) ++ edges.map(e => e.b -> e.size)
    val vertices = edgeWeights.groupBy(_._1).mapValues(_.unzip._2.sum).map {
      case (vid, degree) => vid -> Vertex(vid, degree)
    }.toMap
    for (_ <- 0 to Iterations) {
      for (e <- edges) {
        val a = vertices(e.a)
        val b = vertices(e.b)
        val d = b.pos - a.pos
        val attraction = d * d.len * Fraction / IdealDistance
        a.pos += attraction / a.mass
        b.pos -= attraction / b.mass
      }
      for (a <- vertices.values; b <- vertices.values; if a != b) {
        val d = b.pos - a.pos
        val l = d.len
        val repulsion =
          if (l < 0.01 * IdealDistance) randomVector(a.id + b.id)
          else d * Fraction * IdealDistance * IdealDistance / l / l
        a.pos -= repulsion / a.mass
        b.pos += repulsion / b.mass
      }
    }
    edges.map {
      e => e.copy(aPos = Some(vertices(e.a).pos), bPos = Some(vertices(e.b).pos))
    }
  }

  implicit class VectorOps(v: FE3DPosition) {
    def *(c: Double): FE3DPosition = FE3DPosition(v.x * c, v.y * c, v.z * c)
    def /(c: Double): FE3DPosition = v * (1.0 / c)
    def +(v2: FE3DPosition): FE3DPosition = FE3DPosition(v.x + v2.x, v.y + v2.y, v.z + v2.z)
    def -(v2: FE3DPosition): FE3DPosition = FE3DPosition(v.x - v2.x, v.y - v2.y, v.z - v2.z)
    def len: Double = Math.sqrt(v.x * v.x + v.y * v.y + v.z * v.z)
  }

  def randomVector(seed: Int): FE3DPosition = {
    val phase = seed.toDouble
    FE3DPosition(Math.sin(phase), Math.sin(phase * 2.0), Math.sin(phase * 3.0))
  }
}
