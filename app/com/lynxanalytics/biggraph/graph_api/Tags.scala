// Tags are a small in-memory filesystem that stores projects as directories.
package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.util.UUID
import scala.collection.mutable

import com.lynxanalytics.biggraph.graph_util.{ LoggedEnvironment, Timestamp }

class SymbolPath(val path: Iterable[Symbol]) extends Iterable[Symbol] with Ordered[SymbolPath] {
  override def equals(p: Any) = {
    p.isInstanceOf[SymbolPath] && path == p.asInstanceOf[SymbolPath].path
  }
  override def hashCode = toString.hashCode
  def /(symbol: Symbol): SymbolPath = {
    val expanded = path.toSeq :+ symbol
    SymbolPath.check(symbol, expanded)
    expanded
  }
  def /:(symbol: Symbol): SymbolPath = {
    val expanded = symbol +: path.toSeq
    SymbolPath.check(symbol, expanded)
    expanded
  }
  def /(suffixPath: SymbolPath): SymbolPath = path ++ suffixPath
  def /(suffixDir: String): SymbolPath = /(Symbol(suffixDir))
  def /:(prefixDir: String): SymbolPath = /:(Symbol(prefixDir))
  override def toString = path.map(_.name).mkString("/")
  def iterator = path.iterator
  def parent: SymbolPath = path.init
  def name = path.last
  def compare(other: SymbolPath) = toString compare other.toString
}
object SymbolPath {
  import scala.language.implicitConversions
  implicit def fromIterable(sp: Iterable[Symbol]): SymbolPath = new SymbolPath(sp)
  def parse(str: String): SymbolPath = {
    if (str.isEmpty) Nil
    else str.split("/", -1).toSeq.map(Symbol(_))
  }
  def check(symbol: Symbol, fullPath: SymbolPath) = {
    val str = symbol.name
    assert(!str.contains("/"), s"Name $str in $fullPath contains a slash ('/').")
    assert(str.nonEmpty, s"Path '$fullPath' contains an empty name.")
  }

  def apply(first: Symbol, optional: Symbol*): SymbolPath = {
    val path = first +: optional
    val newPath = new SymbolPath(path)
    for (name <- path) check(name, newPath)
    newPath
  }

  def apply(first: String, optional: String*): SymbolPath = {
    apply(Symbol(first), optional.map(Symbol(_)): _*)
  }

  def apply(): SymbolPath = {
    new SymbolPath(Seq())
  }
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

  def isDir = false

  // A string snapshot of the full directory structure. Mostly for debugging.
  def lsRec(indent: Int = 0): String
}
object TagPath {
  import scala.language.implicitConversions
  implicit def asDir(path: TagPath) = path.asInstanceOf[TagDir]
  implicit def asTag(path: TagPath) = path.asInstanceOf[Tag]
}

final case class Tag(name: Symbol, parent: TagDir, content: String) extends TagPath {
  def clone(newParent: TagDir, newName: Symbol): Tag = newParent.addTag(newName, content)
  def followPath(names: Iterable[Symbol]): Option[TagPath] =
    if (names.nonEmpty) None else Some(this)
  def allTags = Seq(this)
  def lsRec(indent: Int = 0): String = " " * indent + fullName + " => " + content + "\n"
  def gUID: UUID = UUID.fromString(content)
}

trait TagDir extends TagPath {
  protected val store: KeyValueStore

  def /(subPath: SymbolPath): TagPath = {
    val p = followPath(subPath)
    assert(p.nonEmpty, s"$subPath not found in $this")
    p.get
  }

  override def isDir = true

  def exists(subPath: SymbolPath) = followPath(subPath).nonEmpty
  def existsDir(subPath: SymbolPath) = followPath(subPath).exists(_.isInstanceOf[TagDir])
  def existsTag(subPath: SymbolPath) = followPath(subPath).exists(_.isInstanceOf[Tag])

  def rmChild(name: Symbol): Unit = synchronized {
    children(name) match {
      case tag: Tag =>
        store.delete(tag.fullName.toString)
      case dir: TagDir =>
        store.deletePrefix(dir.fullName.toString + "/")
    }
    children -= name
  }
  def rm(offspring: SymbolPath): Unit = synchronized {
    followPath(offspring).map(_.rm())
  }
  def addTag(name: Symbol, content: String): Tag = synchronized {
    assert(!children.contains(name), s"'$this' already contains '$name'.")
    val result = Tag(name, this, content)
    store.put(result.fullName.toString, content)
    children(name) = result
    result
  }
  def setTag(path: SymbolPath, content: String): Tag = synchronized {
    assert(!existsDir(path), s"'$path' is a directory.")
    assert(path.nonEmpty, s"Cannot create path named '$path'.")
    if (existsTag(path)) rm(path)
    val dir = mkDirs(new SymbolPath(path.dropRight(1)))
    dir.addTag(path.last, content)
  }

  def mkDir(name: Symbol): TagSubDir = synchronized {
    val tagName = SymbolPath(name)
    assert(!existsTag(tagName), s"Tag '$name' already exists.")
    if (existsDir(tagName)) return (this / tagName).asInstanceOf[TagSubDir]
    val result = TagSubDir(name, this, store)
    children(name) = result
    result
  }
  def mkDirs(path: SymbolPath): TagDir = synchronized {
    if (path.isEmpty) this
    else mkDir(path.head).mkDirs(new SymbolPath(path.tail))
  }

  def cp(from: SymbolPath, to: SymbolPath): TagPath = synchronized {
    assert(to.nonEmpty, s"Cannot copy from '$from' to '$to'.")
    assert(!exists(to), s"Path '$to' already exists.")
    assert(exists(from), s"Path '$from' does not exist.")
    val toDir = mkDirs(to.dropRight(1))
    (this / from).clone(toDir, to.last)
  }

  def followPath(names: Iterable[Symbol]): Option[TagPath] = {
    if (names.isEmpty) Some(this)
    else children.get(names.head).flatMap(_.followPath(names.tail))
  }

  def clone(newParent: TagDir, newName: Symbol): TagSubDir = {
    assert(!newParent.isOffspringOf(this), s"'$newParent' contains '$this'.")
    val cloned = newParent.mkDir(newName)
    for ((name, child) <- children) {
      child.clone(cloned, name)
    }
    cloned
  }

  def allTags = children.values.flatMap(_.allTags)

  def lsRec(indent: Int = 0): String =
    " " * indent + fullName + "\n" + children.values.map(_.lsRec(indent + 1)).mkString

  def clear(): Unit = synchronized {
    store.deletePrefix(fullName.toString + "/")
    children.clear()
  }

  def ls: Seq[TagPath] = children.values.toSeq.sorted

  private val children = mutable.Map[Symbol, TagPath]()
}

final case class TagSubDir(name: Symbol, parent: TagDir, store: KeyValueStore) extends TagDir

final case class TagRoot(protected val store: KeyValueStore) extends TagDir {
  val name = null
  val parent = null
  override val fullName: SymbolPath = new SymbolPath(Seq())

  override def isOffspringOf(other: TagPath): Boolean = (other == this)

  def batch[T](fn: => T): T = store.batch(fn)

  def writesCanBeIgnored[T](fn: => T): T = store.writesCanBeIgnored(fn)

  def setTags(tags: Map[SymbolPath, String]): Unit = synchronized {
    batch {
      for ((k, v) <- tags) {
        setTag(k, v)
      }
    }
  }

  def gUID(path: SymbolPath): UUID = (this / path).gUID

  // Create tags from the key-value store.
  store.writesCanBeIgnored {
    setTags(TagRoot.loadFromStore(store))
  }
}
object TagRoot {
  val journalFilename = "tags.journal"

  def loadFromRepo(repo: String): Map[SymbolPath, String] =
    loadFromStore(storeFromRepo(repo))

  private def loadFromStore(store: KeyValueStore): Map[SymbolPath, String] =
    store.readAll.map { case (k, v) => SymbolPath.parse(k) -> v }.toMap

  private def storeFromRepo(repo: String): KeyValueStore = {
    val tagsJournal = new File(repo, journalFilename)
    val tagsOld = new File(repo, "tags")
    if (tagsJournal.exists) {
      new JournalKeyValueStore(tagsJournal.toString)
    } else if (tagsOld.exists) {
      new JsonKeyValueStore(tagsOld.toString)
    } else { // Nothing to load. Use a journal.
      new JournalKeyValueStore(tagsJournal.toString)
    }
  }

  // Creates an empty root backed in a tmp directory.
  def temporaryRoot: TagRoot = {
    val tmpRootDir = LoggedEnvironment.envOrElse("KITE_LOCAL_TMP", "/tmp")
    val currentRepo = s"$tmpRootDir/$Timestamp"
    apply(currentRepo)
  }

  def apply(repo: String): TagRoot = {
    val oldStore = storeFromRepo(repo) // May be from earlier versions.
    val tagsJournal = new File(repo, journalFilename)
    val newStore = new JournalKeyValueStore(tagsJournal.toString)
    val root = new TagRoot(newStore)
    if (oldStore != newStore) {
      root.setTags(loadFromStore(oldStore))
    }
    root
  }
}
