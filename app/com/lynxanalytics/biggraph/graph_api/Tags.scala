package com.lynxanalytics.biggraph.graph_api

import java.util.UUID
import scala.collection.mutable

class SymbolPath(val path: Iterable[Symbol]) extends Iterable[Symbol] with Ordered[SymbolPath] {
  def /(name: Symbol): SymbolPath = path.toSeq :+ name
  def /(suffixPath: SymbolPath): SymbolPath = path ++ suffixPath
  override def toString = path.map(_.name).mkString("/")
  def iterator = path.iterator
  def parent: SymbolPath = path.init
  def name = path.last
  def compare(other: SymbolPath) = toString compare other.toString
}
object SymbolPath {
  import scala.language.implicitConversions
  implicit def fromIterable(sp: Iterable[Symbol]): SymbolPath = new SymbolPath(sp)
  implicit def fromString(str: String): SymbolPath =
    str.split("/").toSeq.map(Symbol(_))
  implicit def fromSymbol(sym: Symbol): SymbolPath = fromString(sym.name)
}

sealed trait TagPath extends Serializable with Ordered[TagPath] {
  val name: Symbol
  val parent: TagDir

  def fullName: SymbolPath = parent.fullName / name

  def compare(other: TagPath) = fullName compare other.fullName

  def rm() = parent.rmChild(name)

  def isOffspringOf(other: TagPath): Boolean =
    (other == this) || this.parent.isOffspringOf(other)

  def clone(newParent: TagDir, newName: Symbol): TagPath

  def followPath(names: Iterable[Symbol]): Option[TagPath]

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
  def followPath(names: Iterable[Symbol]): Option[TagPath] =
    if (names.nonEmpty) None else Some(this)
  def allTags = Seq(this)
  def lsRec: String = fullName + " => " + gUID + "\n"
}

trait TagDir extends TagPath {
  def /(subPath: SymbolPath): TagPath = {
    val p = followPath(subPath)
    assert(p.nonEmpty, s"$subPath not found in $this")
    p.get
  }

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
    assert(path.nonEmpty)
    if (existsTag(path)) rm(path)
    val dir = mkDirs(new SymbolPath(path.dropRight(1)))
    dir.addTag(path.last, value)
  }

  def mkDir(name: Symbol): TagSubDir = synchronized {
    assert(!existsTag(name))
    if (existsDir(name)) return (this / name).asInstanceOf[TagSubDir]
    val result = TagSubDir(name, this)
    children(name) = result
    result
  }
  def mkDirs(path: SymbolPath): TagDir = synchronized {
    if (path.isEmpty) this
    else mkDir(path.head).mkDirs(new SymbolPath(path.tail))
  }

  def cp(from: SymbolPath, to: SymbolPath): TagPath = synchronized {
    assert(to.nonEmpty)
    assert(!exists(to))
    assert(exists(from))
    val toDir = mkDirs(to.dropRight(1))
    (this / from).clone(toDir, to.last)
  }

  def followPath(names: Iterable[Symbol]): Option[TagPath] = {
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

  def ls: Seq[TagPath] = children.values.toSeq.sorted

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

