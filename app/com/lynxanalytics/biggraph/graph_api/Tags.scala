package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import scala.collection.mutable

class SymbolPath(val path: Seq[Symbol]) {
  def /(name: Symbol): SymbolPath = new SymbolPath(path :+ name)
}
object SymbolPath {
  import scala.language.implicitConversions
  implicit def asSeq(sp: SymbolPath): Seq[Symbol] = sp.path
  implicit def fromString(str: String): SymbolPath =
    new SymbolPath(str.split("/").toSeq.map(Symbol(_)))
  implicit def fromSymbol(sym: Symbol): SymbolPath = fromString(sym.toString)
}

sealed trait TagPath extends Serializable {
  val name: Symbol
  val parent: TagDir

  def fullName = Symbol(f"$parent.fullName/$name")

  def isOffspringOf(other: TagPath): Boolean =
    (other == this) || this.parent.isOffspringOf(other)

  def clone(newParent: TagDir, newName: Symbol): TagPath
}
object TagPath {
  import scala.language.implicitConversions
  implicit def asDir(path: TagPath) = path.asInstanceOf[TagDir]
  implicit def asTag(path: TagPath) = path.asInstanceOf[Tag]
}

trait TagDir extends TagPath {
  def /(subPath: SymbolPath): TagPath =
    followPath(subPath)

  def rm(name: Symbol): Unit = children -= name

  def addTag(name: Symbol, value: UUID): Tag = synchronized {
    assert(!children.contains(name))
    val result = Tag(name, this, value)
    children(name) = result
    return result
  }

  def mkDir(name: Symbol): TagDir = synchronized {
    assert(!children.contains(name))
    val result = TagSubDir(name, this)
    children(name) = result
    return result
  }

  private def followPath(names: Seq[Symbol]): TagPath =
    if (names.isEmpty) this
    else (children(names.head).asInstanceOf[TagDir]).followPath(names.tail)

  def clone(newParent: TagDir, newName: Symbol): TagSubDir = {
    assert(!newParent.isOffspringOf(this))
    ???
  }
  private val children = mutable.Map[Symbol, TagPath]()
}

final case class TagRoot() extends TagDir {
  val name = Symbol("")
  val parent = null
  override val fullName = name
  override def isOffspringOf(other: TagPath): Boolean = (other == this)
}

final case class TagSubDir(name: Symbol, parent: TagDir) extends TagDir {
}

final case class Tag(name: Symbol, parent: TagDir, gUID: UUID) extends TagPath {
  def clone(newParent: TagDir, newName: Symbol): Tag = newParent.addTag(newName, gUID)
}
