package com.lynxanalytics.biggraph.graph_operations

import java.io.File

import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_api._
import org.geotools.data.FileDataStoreFinder
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.scalatest.FunSuite

class ShapeFileTest extends FunSuite with TestGraphOp {
  test("shapefile shit") {
//    val finder = FileDataStoreFinder.getDataStore(new File("../Downloads/shapefile/GIH3_DC_2015_POLY.shp"))
    val finder = FileDataStoreFinder.getDataStore(new File("../Downloads/hk-epsg-4326/hk.shp"))
    val iterator = finder.getFeatureSource.getFeatures.features()

    finder.getFeatureSource.getFeatures()
    while (iterator.hasNext) {
      val value = iterator.next()

      if (value.getDefaultGeometryProperty.getBounds.contains(114.155700008, 22.3611180163))
        println(value.getAttribute("DISTRICT_E"))
    }

  }
}
