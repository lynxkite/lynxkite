package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite

class HadoopFileTest extends FunSuite {
  val rootPath = getClass.getResource("/graph_util/hadoop_tests").toString
  RootRepository.registerRoot("HADOOPTEST$", rootPath)

  test("Test basic RootRepository asserts") {
    RootRepository.registerRoot("BABABA$", "mamam")
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("BABABA$", "mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("KJHKJSDDSJ@", "mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("KJHKJSDDSJ$/haha", "mamam")
    }
  }

  test("Password setting works") {
    val dummy = RootRepository.getDummyRootName("s3n://access:secret@lynx-bnw-test2")
    val dataFile = HadoopFile(dummy + "/somedir/somefile")
    val conf = dataFile.hadoopConfiguration()
    assert(conf.get("fs.s3n.awsAccessKeyId") == "access")
    assert(conf.get("fs.s3n.awsSecretAccessKey") == "secret")
  }

  test("Path concatenation works") {
    val dummy = RootRepository.getDummyRootName("s3n://access:secret@lynx-bnw-test2")
    val d = HadoopFile(dummy) / "dir/file"
    assert(d.resolvedName == "s3n://lynx-bnw-test2/dir/file")
    val q = d + ".ext"
    assert(q.resolvedName == "s3n://lynx-bnw-test2/dir/file.ext")
  }

  def wildcardTest(resourceRoot: HadoopFile) = {
    val all = resourceRoot / "*"
    assert(all.list.length == 5)
    val txt = resourceRoot / "*.txt"
    assert(txt.list.length == 3)
  }

  test("Wildcard matching works") {
    wildcardTest(HadoopFile("HADOOPTEST$"))
    val f = HadoopFile("HADOOPTEST$") / "*"
    assert(f.list.length == 5)
    val g = HadoopFile("HADOOPTEST$/*.txt")
    assert(g.list.length == 3)
  }

  test("Hadoop forward-backward conversion works") {

    RootRepository.registerRoot("HADOOPROOTA$", "file:/home/rootdir")
    val f1 = HadoopFile("HADOOPROOTA$/subdir") / "*"
    val g1 = f1.copyUpdateRelativePath("file:/home/rootdir/subdir/file")
    assert(g1.symbolicName == "HADOOPROOTA$/subdir/file")

    RootRepository.registerRoot("HADOOPROOTB$", "s3n://key:secret@rootdir")
    val f2 = HadoopFile("HADOOPROOTB$/subdir") / "*"
    val g2 = f2.copyUpdateRelativePath("s3n://rootdir/subdir/file")
    assert(g2.symbolicName == "HADOOPROOTB$/subdir/file")

    RootRepository.registerRoot("HADOOPROOTC$", "s3n://key:s")
    val f3 = HadoopFile("HADOOPROOTC$ecret@rootdir/subdir1/file")
    assert(f3.relativePath == "ecret@rootdir/subdir1/file")
    val g3 = f3.copyUpdateRelativePath("s3n://rootdir/subdir1/file")
    assert(g3.awsID == "key")
    assert(g3.awsSecret == "secret")
  }

  test("Empty symbolic prefix works with file:// scheme") {
    RootRepository.registerRoot("EMPTYFILE$", "")
    val resourceDir = HadoopFile("EMPTYFILE$") + rootPath
    println(rootPath)
    println(resourceDir.resolvedNameWithCredentials)
    println(resourceDir.relativePath)
    println(resourceDir.toString)
    wildcardTest(resourceDir)
  }
}
