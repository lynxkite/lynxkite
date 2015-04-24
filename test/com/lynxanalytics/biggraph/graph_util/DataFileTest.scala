package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

class DataFileTest extends FunSuite {

  test("Test basic RootRepository asserts") {
    RootRepository.registerRoot("$BABABA", "mamam")
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("$BABABA", "mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("@KJHKJSDDSJ", "mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("$KJHKJSDDSJ/haha", "mamam")
    }
  }

  test("Test basic RootRepository logic") {
    RootRepository.registerRoot("$HELLO", "/bello")
    assert(RootRepository.getRootInfo("$HELLO").resolution == "/bello")
    RootRepository.registerRoot("$UPLOAD", "$HELLO/uploads")
    assert(RootRepository.getRootInfo("$UPLOAD").resolution == "/bello/uploads")

    RootRepository.registerRoot("$AWAY", "s3n://access:secret@lynx-bnw-data")
    val r = RootRepository.getRootInfo("$AWAY")
    assert(r.resolution == "s3n://lynx-bnw-data")
    assert(r.accessKey == "access")
    assert(r.secretKey == "secret")

    RootRepository.registerRoot("$FARAWAY", "$AWAY/uploads")
    val rr = RootRepository.getRootInfo("$FARAWAY")
    assert(rr.resolution == "s3n://lynx-bnw-data/uploads")
    assert(rr.accessKey == "access")
    assert(rr.secretKey == "secret")
  }

}
