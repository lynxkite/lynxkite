package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

class HadoopFileTest extends FunSuite {

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
  /*
  test("Test basic RootRepository logic") {
    RootRepository.registerRoot("$HELLO", "/bello")
    assert(RootRepository.getRootInfo("$HELLO").resolution == "/bello")
    RootRepository.registerRoot("$TESTUPLOAD", "$HELLO/uploads")
    assert(RootRepository.getRootInfo("$TESTUPLOAD").resolution == "/bello/uploads")

    RootRepository.registerRoot("$AWAY", "s3n://access:secret@lynx-bnw-test")
    val r = RootRepository.getRootInfo("$AWAY")
    assert(r.resolution == "s3n://lynx-bnw-test")
    assert(r.accessKey == "access")
    assert(r.secretKey == "secret")

    RootRepository.registerRoot("$FARAWAY", "$AWAY/uploads")
    val rr = RootRepository.getRootInfo("$FARAWAY")
    assert(rr.resolution == "s3n://lynx-bnw-test/uploads")
    assert(rr.accessKey == "access")
    assert(rr.secretKey == "secret")
  }
*/
  test("Password setting works") {
    val dummy = RootRepository.getDummyRootName("s3n://access:secret@lynx-bnw-test")
    val dataFile = HadoopFile(dummy + "/somedir/somefile")
    val conf = dataFile.hadoopConfiguration()
    assert(conf.get("fs.s3n.awsAccessKeyId") == "access")
    assert(conf.get("fs.s3n.awsSecretAccessKey") == "secret")
  }

}
