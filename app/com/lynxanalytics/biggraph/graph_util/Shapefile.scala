// Utility class for Shapefiles.
package com.lynxanalytics.biggraph.graph_util

import java.io.File
import org.geotools.data.FileDataStoreFinder
import org.geotools.data.simple.SimpleFeatureIterator
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object Shapefile {
  // This class makes it possible to use scalaic tools on SimpleFeatureIterators automatically
  implicit class ScalaFeatureIterator(it: SimpleFeatureIterator) extends Iterator[SimpleFeature] {
    override def hasNext: Boolean = it.hasNext
    override def next(): SimpleFeature = it.next()
  }
}

// A helper class for Shapefiles. Provides immutable attribute metadata, one iterator for the
// features in the Shapefile plus utility functions.
case class Shapefile(filename: String) {

  private val dataStore = FileDataStoreFinder.getDataStore(new File(filename))

  // TODO(gsvigruha): Shapefiles come with a default, often complex geometry attribute. Maybe remove
  // that default geometry attribute from here and only keep simple ones (strings, numbers).
  val attrNames =
    dataStore.getSchema().getAttributeDescriptors().map(attr => attr.getLocalName()).toIndexedSeq
  val iterator = dataStore.getFeatureSource.getFeatures().features()

  def assertHasAttributeName(name: String): Unit = {
    assert(attrNames.contains(name), s"Attribute name $name does not exist in Shapefile. " +
      s"Available attributes are ${attrNames mkString ", "}.")
  }

  def close(): Unit = {
    iterator.close()
    dataStore.dispose()
  }
}

