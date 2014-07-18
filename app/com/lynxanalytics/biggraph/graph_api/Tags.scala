package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import scala.collection.mutable

class SymbolPath(val path: Seq[Symbol]) {
  def /(name: Symbol): SymbolPath = path :+ name
  def /(suffixPath: SymbolPath): SymbolPath = path ++ suffixPath.path
  override def toString = path.map(_.name).mkString("/")
}
object SymbolPath {
  import scala.language.implicitConversions
  implicit def fromSeq(sp: Seq[Symbol]): SymbolPath = new SymbolPath(sp)
  implicit def fromString(str: String): SymbolPath =
    str.split("/").toSeq.map(Symbol(_))
  implicit def fromSymbol(sym: Symbol): SymbolPath = fromString(sym.name)
}

sealed trait TagPath extends Serializable {
  val name: Symbol
  val parent: TagDir

  def fullName: SymbolPath = parent.fullName / name

  def rm() = parent.rmChild(name)

  def isOffspringOf(other: TagPath): Boolean =
    (other == this) || this.parent.isOffspringOf(other)

  def clone(newParent: TagDir, newName: Symbol): TagPath

  def followPath(names: Seq[Symbol]): Option[TagPath]

  def allTags: Iterable[Tag]

  // A string snapshot of the full directory structure. Mostly for debugging.
  def lsRec: String
}
object TagPath {
  import scala.language.implicitConversions
  implicit def asDir(path: TagPath) = path.asInstanceOf[TagDir]
  implicit def asTag(path: TagPath) = path.asInstanceOf[Tag]
}

final case class Tag(name: Symbol, parent: TagDir, gUID: UUID) extends TagPath {
  def clone(newParent: TagDir, newName: Symbol): Tag = newParent.addTag(newName, gUID)
  def followPath(names: Seq[Symbol]): Option[TagPath] =
    if (names.nonEmpty) None else Some(this)
  def allTags = Seq(this)
  def lsRec: String = fullName + " => " + gUID + "\n"
}

trait TagDir extends TagPath {
  def /(subPath: SymbolPath): TagPath =
    followPath(subPath).get

  def exists(subPath: SymbolPath) = followPath(subPath).nonEmpty
  def existsDir(subPath: SymbolPath) = followPath(subPath).exists(_.isInstanceOf[TagDir])
  def existsTag(subPath: SymbolPath) = followPath(subPath).exists(_.isInstanceOf[Tag])

  def rmChild(name: Symbol): Unit = synchronized {
    children -= name
  }
  def rm(offspring: SymbolPath): Unit = synchronized {
    followPath(offspring).map(_.rm())
  }
  def addTag(name: Symbol, value: UUID): Tag = synchronized {
    assert(!children.contains(name))
    val result = Tag(name, this, value)
    children(name) = result
    result
  }
  def setTag(path: SymbolPath, value: UUID): Tag = synchronized {
    assert(!existsDir(path))
    val seq = path.path
    assert(seq.nonEmpty)
    if (existsTag(path)) rm(path)
    val dir = mkDirs(new SymbolPath(seq.dropRight(1)))
    dir.addTag(seq.last, value)
  }

  def mkDir(name: Symbol): TagSubDir = synchronized {
    assert(!existsTag(name))
    if (existsDir(name)) return (this / name).asInstanceOf[TagSubDir]
    val result = TagSubDir(name, this)
    children(name) = result
    result
  }
  def mkDirs(path: SymbolPath): TagDir = synchronized {
    val seq = path.path
    if (seq.isEmpty) this
    else mkDir(seq.head).mkDirs(new SymbolPath(seq.tail))
  }

  def cp(from: SymbolPath, to: SymbolPath): TagPath = synchronized {
    val toSeq = to.path
    assert(toSeq.nonEmpty)
    assert(!exists(to))
    assert(exists(from))
    val toDir = mkDirs(toSeq.dropRight(1))
    (this / from).clone(toDir, toSeq.last)
  }

  def followPath(path: SymbolPath): Option[TagPath] = followPath(path.path)
  def followPath(names: Seq[Symbol]): Option[TagPath] = {
    if (names.isEmpty) Some(this)
    else children.get(names.head).flatMap(_.followPath(names.tail))
  }

  def clone(newParent: TagDir, newName: Symbol): TagSubDir = {
    assert(!newParent.isOffspringOf(this))
    val cloned = newParent.mkDir(newName)
    children.foreach { case (name, child) => child.clone(cloned, name) }
    cloned
  }

  def allTags = children.values.flatMap(_.allTags)

  def lsRec: String = fullName + "\n" + children.values.map(_.lsRec).mkString

  def clear(): Unit = children.clear()

  private val children = mutable.Map[Symbol, TagPath]()
}

final case class TagSubDir(name: Symbol, parent: TagDir) extends TagDir {
}

object TagRoot {
  private val tagRE = "(.*):([^:]*)".r
}
final case class TagRoot() extends TagDir {
  val name = null
  val parent = null
  override val fullName: SymbolPath = new SymbolPath(Seq())
  override def isOffspringOf(other: TagPath): Boolean = (other == this)
  def saveToString: String =
    allTags.map(tag => "%s:%s".format(tag.fullName, tag.gUID)).mkString("\n")
  def loadFromString(data: String) = {
    clear()
    data.split("\n").foreach {
      _ match {
        case TagRoot.tagRE(name, value) => setTag(name, UUID.fromString(value))
      }
    }
  }
}

