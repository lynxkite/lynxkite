// Generates 3D positions for FEEdge endpoints.
package com.lynxanalytics.biggraph.controllers

object ForceLayout3D {
  case class Vertex(id: Int, mass: Double, var pos: FE3DPosition)

  final val IdealDistance = 10.0
  final val Fraction = 0.1
  final val Iterations = 50
  final val Gravity = 0.01

  // The basic formula is:
  //   attraction = distance ^ 2 / ideal_distance
  //   repulsion = ideal_distance ^ 2 / distance
  // The idea is that these two cancel out at the ideal distance.
  def apply(edges: Seq[FEEdge]): Map[String, FE3DPosition] = {
    val edgeWeights = edges.map(e => e.a -> e.size) ++ edges.map(e => e.b -> e.size)
    val vertices = edgeWeights.groupBy(_._1).mapValues(_.unzip._2.sum).map {
      case (vid, degree) => vid -> Vertex(vid, degree + 1, randomVector(vid))
    }.toMap
    for (i <- 0 to Iterations) {
      for (e <- edges) {
        val a = vertices(e.a)
        val b = vertices(e.b)
        val d = b.pos - a.pos
        // Avoid overshooting and divergent oscillations.
        val attraction = d * math.min(0.4, d.len * Fraction / IdealDistance)
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
      for (a <- vertices.values) {
        a.pos -= a.pos * Gravity
      }
    }
    vertices.map { case (k, v) => k.toString -> v.pos }
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
