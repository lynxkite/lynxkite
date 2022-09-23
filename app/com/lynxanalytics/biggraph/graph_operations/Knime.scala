// Runs a KNIME node as an operation.
package com.lynxanalytics.biggraph.graph_operations

import scala.collection.JavaConverters._

object Knime {
  val ctx = org.eclipse.core.runtime.adaptor.EclipseStarter.startup(Array(), null)
  val cg = new io.github.classgraph.ClassGraph()
  val files = cg.scan.getClasspathFiles.asScala
  // val bundles = files.map(f => util.Try(ctx.installBundle("file:" + f.getPath)))
  def install(c: Class[_]) = {
    val path = c.getName.replace(".", "/")
    val url = c.getResource(s"/$path.class")
    val jar = url.getPath.split("!").head
    ctx.installBundle(jar)
  }
  val systemBundle = install(classOf[org.eclipse.core.internal.runtime.InternalPlatform])
  systemBundle.start()
  def exm = org.knime.core.node.extension.NodeFactoryExtensionManager.getInstance
    def main(args: Array[String]): Unit = {
      println("hello")
    }

}
