package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.TestUtils
import org.scalatest.FunSuite

class HadoopFileTest extends FunSuite {
  val rootPath = getClass.getResource("/graph_util/hadoop_tests").toString
  RootRepository.registerRoot("HADOOPTEST$", rootPath)

  test("Test basic RootRepository asserts") {
    RootRepository.registerRoot("BABABA$", "x:mamam")
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("BABABA$", "x:mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("KJHKJSDDSJ@", "x:mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("KJHKJSDDSJ$/haha", "x:mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("NOSCHEME$", "noschemetobefoundhere/alma")
    }
    RootRepository.registerRoot("_$", "")
    RootRepository.registerRoot("AB_$", "")
    RootRepository.registerRoot("A1$", "")
    RootRepository.registerRoot("QQ$", "")
    RootRepository.registerRoot("Q012$", "")
    RootRepository.registerRoot("P4Q1$", "")
    RootRepository.registerRoot("W$", "")

    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("9$", "")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("3P4$", "")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("$", "")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("A$B", "")
    }
  }

  test("Password setting works") {
    val dummy = TestUtils.getDummyRootName("s3n://access:secret@lynx-bnw-test2")
    val dataFile = HadoopFile(dummy + "/somedir/somefile")
    val conf = dataFile.hadoopConfiguration()
    assert(conf.get("fs.s3n.awsAccessKeyId") == "access")
    assert(conf.get("fs.s3n.awsSecretAccessKey") == "secret")
  }

  test("Path concatenation works") {
    val dummy = TestUtils.getDummyRootName("s3n://access:secret@lynx-bnw-test2")
    val d = HadoopFile(dummy) / "dir/file"
    assert(d.resolvedNameWithNoCredentials == "s3n://lynx-bnw-test2/dir/file")
    val q = d + ".ext"
    assert(q.resolvedNameWithNoCredentials == "s3n://lynx-bnw-test2/dir/file.ext")
  }

  def wildcardTest(resourceRoot: HadoopFile) = {
    val all = resourceRoot / "*"
    assert(all.list.length == 5)
    val txt = resourceRoot / "*.txt"
    assert(txt.list.length == 3)
  }

  test("Wildcard matching works") {
    wildcardTest(HadoopFile("HADOOPTEST$"))
  }

  test("Hadoop normalization works for operator /") {
    RootRepository.registerRoot("ROOT1$", "file:/home/rootdir/")
    val f1 = HadoopFile("ROOT1$") / "file.txt"
    assert(f1.symbolicName == "ROOT1$file.txt")
    assert(f1.resolvedName == "file:/home/rootdir/file.txt")

    RootRepository.registerRoot("ROOT2$", "file:/home/rootdir")
    val f2 = HadoopFile("ROOT2$") / "file.txt"
    assert(f2.symbolicName == "ROOT2$/file.txt")
    assert(f2.resolvedName == "file:/home/rootdir/file.txt")

  }

  test("Hadoop forward-backward conversion works") {

    RootRepository.registerRoot("HADOOPROOTA$", "file:/home/rootdir")
    val f1 = HadoopFile("HADOOPROOTA$/subdir") / "*"
    val g1 = f1.hadoopFileForGlobOutput("file:/home/rootdir/subdir/file")
    assert(g1.symbolicName == "HADOOPROOTA$/subdir/file")

    RootRepository.registerRoot("HADOOPROOTB$", "s3n://key:secret@rootdir")
    val f2 = HadoopFile("HADOOPROOTB$/subdir") / "*"
    val g2 = f2.hadoopFileForGlobOutput("s3n://rootdir/subdir/file")
    assert(g2.symbolicName == "HADOOPROOTB$/subdir/file")

    RootRepository.registerRoot("HADOOPROOTC$", "s3n:/")
    val f3 = HadoopFile("HADOOPROOTC$/key:secret@rootdir/subdir1/file")
    assert(f3.normalizedRelativePath == "/key:secret@rootdir/subdir1/file")
    val g3 = f3.hadoopFileForGlobOutput("s3n://rootdir/subdir1/file")
    assert(g3.awsID == "key")
    assert(g3.awsSecret == "secret")

    RootRepository.registerRoot("HADOOPROOTD$", "s3n://")
    val f4 = HadoopFile("HADOOPROOTD$key:secret@rootdir/subdir1/file")
    assert(f4.normalizedRelativePath == "key:secret@rootdir/subdir1/file")
    val g4 = f4.hadoopFileForGlobOutput("s3n://rootdir/subdir1/file")
    assert(g4.awsID == "key")
    assert(g4.awsSecret == "secret")

    RootRepository.registerRoot("HADOOP_ROOT$", "s3n:")
    val f5 = HadoopFile("HADOOP_ROOT$//key:secret@rootdir/subdir1/file")
    assert(f5.normalizedRelativePath == "//key:secret@rootdir/subdir1/file")
    val g5 = f5.hadoopFileForGlobOutput("s3n://rootdir/subdir1/file")
    assert(g5.awsID == "key")
    assert(g5.awsSecret == "secret")

    RootRepository.registerRoot("HADOOP_ROOT1$", "file:/home///user/")
    val f6 = HadoopFile("HADOOP_ROOT1$file.txt")
    assert(f6.normalizedRelativePath == "file.txt")
    val g6 = f6.hadoopFileForGlobOutput("file:/home/user/file.txt")
    assert(g6.symbolicName == "HADOOP_ROOT1$file.txt")

    RootRepository.registerRoot("HADOOP_ROOT2$", "file:/home///user/")
    val f7 = HadoopFile("HADOOP_ROOT2$/file.txt")
    assert(f7.normalizedRelativePath == "file.txt")
    val g7 = f7.hadoopFileForGlobOutput("file:/home/user/file.txt")
    assert(g7.symbolicName == "HADOOP_ROOT2$file.txt")

  }

  test("Empty symbolic prefix works with file:// scheme") {
    RootRepository.registerRoot("EMPTYFILE$", "")
    val resourceDir = HadoopFile("EMPTYFILE$") + rootPath
    wildcardTest(resourceDir)
  }

  def checkOne(rootSymbol: String, pathAndOutput: Tuple2[String, String]) = {
    val (relativePath, expectedOutput) = (pathAndOutput._1, pathAndOutput._2)
    if (expectedOutput == "ASSERT") {
      intercept[java.lang.AssertionError] {
        HadoopFile(rootSymbol + relativePath)
      }
    } else {
      val file = HadoopFile(rootSymbol + relativePath)
      assert(file.resolvedName == expectedOutput)
    }
  }

  def checkPathRules(rootResolution: String,
                     relativePathsAndExpectedOutputs: List[Tuple2[String, String]]) = {
    val rootSymbol = TestUtils.getDummyRootName(rootResolution, false)
    relativePathsAndExpectedOutputs.foreach { checkOne(rootSymbol, _) }
  }

  test("Dangerous concatenations get caught") {

    checkPathRules("",
      List(
        ("a", "a"),
        ("/haha", "/haha"),
        ("///g///", "/g/"),
        ("", ""),
        ("b///", "b/"),
        ("/user/../trick", "ASSERT")))

    checkPathRules("x:b",
      List(
        ("a", "ASSERT"),
        ("/haha", "x:b/haha"),
        ("///g///", "x:b/g/"),
        ("", "x:b"),
        ("b///", "ASSERT"),
        ("/user/../trick", "ASSERT")))

    checkPathRules("x:b/",
      List(
        ("a", "x:b/a"),
        ("/haha", "x:b/haha"),
        ("///g///", "x:b/g/"),
        ("", "x:b/"),
        ("b///", "x:b/b/"),
        ("/user/../trick", "ASSERT")))

    checkPathRules("s3n://key:secret@",
      List(
        ("a", "s3n://key:secret@a"),
        ("///b", "s3n://key:secret@b"),
        ("/hello..", "ASSERT")
      ))

    checkPathRules("x:alma.",
      List(
        ("a", "ASSERT"),
        ("///b", "x:alma./b"),
        (".trick", "ASSERT")
      ))

    checkPathRules("file:/home",
      List(
        ("/user", "file:/home/user"),
        ("//user", "file:/home/user"),
        ("user", "ASSERT")))

    checkPathRules("x:/home",
      List(
        ("/user", "x:/home/user"),
        ("//user", "x:/home/user"),
        ("user", "ASSERT")))

  }

  test("Check user defined path parsing") {
    val filename = rootPath + "/subdir/user_roots.txt"
    val pairs = RootRepository.parseUserDefinedInputFromURI(filename).toList

    val expected = List(
      "TEST_EMPTY" -> "",
      "TEST_S3N" -> "s3n://testkey:testpwd@",
      "TEST_S3NDIR" -> "s3n://testkey:testpwd@directory/",
      "TESTFILEDIR" -> "file:/home/user/",
      "TESTBLANKS_" -> "hdfs://root/path")
    assert(pairs == expected)
  }

  test("Legacy mode works") {
    def f(savedPath: String, expected: String): Unit = {
      if (expected == "ASSERT") {
        intercept[java.lang.AssertionError] {
          HadoopFile(savedPath, true)
        }
      } else {
        val v = HadoopFile(savedPath, true)
        assert(v.symbolicName == expected)
      }
    }
    RootRepository.dropResolutions()
    f("s3n://testkey:secret@data", "ASSERT")
    f("s3n://testkey:secret@data/uploads/file1", "ASSERT")
    f("s3n://testkey:secret@data/uploads/file2", "ASSERT")
    f("s3n://testkey:secret@data/uploads/subdir/file3", "ASSERT")
    f("s3n://testkey:secret@data/another/subdir/file3", "ASSERT")
    f("hdfs:/data", "ASSERT")
    f("hdfs:/data/uploads/file1", "ASSERT")
    f("hdfs:/data/uploads/file2", "ASSERT")
    f("hdfs:/data/uploads/subdir/file3", "ASSERT")
    f("hdfs:/data/another/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data", "ASSERT")
    f("file:/home/user/kite_data/uploads/file1", "ASSERT")
    f("file:/home/user/kite_data/uploads/file2", "ASSERT")
    f("file:/home/user/kite_data/uploads/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data/another/subdir/file3", "ASSERT")

    // key mismatch
    RootRepository.registerRoot("TEST_S3N_BAD$", "s3n://badkey:badpwd@")
    f("s3n://testkey:secret@data", "ASSERT")
    f("s3n://testkey:secret@data/uploads/file1", "ASSERT")
    f("s3n://testkey:secret@data/uploads/file2", "ASSERT")
    f("s3n://testkey:secret@data/uploads/subdir/file3", "ASSERT")
    f("s3n://testkey:secret@data/another/subdir/file3", "ASSERT")
    f("hdfs:/data", "ASSERT")
    f("hdfs:/data/uploads/file1", "ASSERT")
    f("hdfs:/data/uploads/file2", "ASSERT")
    f("hdfs:/data/uploads/subdir/file3", "ASSERT")
    f("hdfs:/data/another/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data", "ASSERT")
    f("file:/home/user/kite_data/uploads/file1", "ASSERT")
    f("file:/home/user/kite_data/uploads/file2", "ASSERT")
    f("file:/home/user/kite_data/uploads/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data/another/subdir/file3", "ASSERT")

    RootRepository.registerRoot("TEST_S3N$", "s3n://testkey:secret@")
    f("s3n://testkey:secret@data", "TEST_S3N$data")
    f("s3n://testkey:secret@data/uploads/file1", "TEST_S3N$data/uploads/file1")
    f("s3n://testkey:secret@data/uploads/file2", "TEST_S3N$data/uploads/file2")
    f("s3n://testkey:secret@data/uploads/subdir/file3", "TEST_S3N$data/uploads/subdir/file3")
    f("s3n://testkey:secret@data/another/subdir/file3", "TEST_S3N$data/another/subdir/file3")
    f("hdfs:/data", "ASSERT")
    f("hdfs:/data/uploads/file1", "ASSERT")
    f("hdfs:/data/uploads/file2", "ASSERT")
    f("hdfs:/data/uploads/subdir/file3", "ASSERT")
    f("hdfs:/data/another/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data", "ASSERT")
    f("file:/home/user/kite_data/uploads/file1", "ASSERT")
    f("file:/home/user/kite_data/uploads/file2", "ASSERT")
    f("file:/home/user/kite_data/uploads/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data/another/subdir/file3", "ASSERT")

    RootRepository.registerRoot("TEST_SCHEME_ADDITION$", "file:/varr/")
    f("/varr/alma", "TEST_SCHEME_ADDITION$alma")

    RootRepository.registerRoot("TEST_S3N_DATA$", "TEST_S3N$/data")
    f("s3n://testkey:secret@data", "TEST_S3N_DATA$")
    f("s3n://testkey:secret@data/uploads/file1", "TEST_S3N_DATA$/uploads/file1")
    f("s3n://testkey:secret@data/uploads/file2", "TEST_S3N_DATA$/uploads/file2")
    f("s3n://testkey:secret@data/uploads/subdir/file3", "TEST_S3N_DATA$/uploads/subdir/file3")
    f("s3n://testkey:secret@data/another/subdir/file3", "TEST_S3N_DATA$/another/subdir/file3")
    f("hdfs:/data", "ASSERT")
    f("hdfs:/data/uploads/file1", "ASSERT")
    f("hdfs:/data/uploads/file2", "ASSERT")
    f("hdfs:/data/uploads/subdir/file3", "ASSERT")
    f("hdfs:/data/another/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data", "ASSERT")
    f("file:/home/user/kite_data/uploads/file1", "ASSERT")
    f("file:/home/user/kite_data/uploads/file2", "ASSERT")
    f("file:/home/user/kite_data/uploads/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data/another/subdir/file3", "ASSERT")

    RootRepository.registerRoot("UPLOAD$", "TEST_S3N_DATA$/uploads")
    f("s3n://testkey:secret@data", "TEST_S3N_DATA$")
    f("s3n://testkey:secret@data/uploads/file1", "UPLOAD$/file1")
    f("s3n://testkey:secret@data/uploads/file2", "UPLOAD$/file2")
    f("s3n://testkey:secret@data/uploads/subdir/file3", "UPLOAD$/subdir/file3")
    f("s3n://testkey:secret@data/another/subdir/file3", "TEST_S3N_DATA$/another/subdir/file3")
    f("hdfs:/data", "ASSERT")
    f("hdfs:/data/uploads/file1", "ASSERT")
    f("hdfs:/data/uploads/file2", "ASSERT")
    f("hdfs:/data/uploads/subdir/file3", "ASSERT")
    f("hdfs:/data/another/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data", "ASSERT")
    f("file:/home/user/kite_data/uploads/file1", "ASSERT")
    f("file:/home/user/kite_data/uploads/file2", "ASSERT")
    f("file:/home/user/kite_data/uploads/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data/another/subdir/file3", "ASSERT")

    RootRepository.registerRoot("HDFS$", "hdfs:/data")
    f("s3n://testkey:secret@data", "TEST_S3N_DATA$")
    f("s3n://testkey:secret@data/uploads/file1", "UPLOAD$/file1")
    f("s3n://testkey:secret@data/uploads/file2", "UPLOAD$/file2")
    f("s3n://testkey:secret@data/uploads/subdir/file3", "UPLOAD$/subdir/file3")
    f("s3n://testkey:secret@data/another/subdir/file3", "TEST_S3N_DATA$/another/subdir/file3")
    f("hdfs:/data", "HDFS$")
    f("hdfs:/data/uploads/file1", "HDFS$/uploads/file1")
    f("hdfs:/data/uploads/file2", "HDFS$/uploads/file2")
    f("hdfs:/data/uploads/subdir/file3", "HDFS$/uploads/subdir/file3")
    f("hdfs:/data/another/subdir/file3", "HDFS$/another/subdir/file3")
    f("file:/home/user/kite_data", "ASSERT")
    f("file:/home/user/kite_data/uploads/file1", "ASSERT")
    f("file:/home/user/kite_data/uploads/file2", "ASSERT")
    f("file:/home/user/kite_data/uploads/subdir/file3", "ASSERT")
    f("file:/home/user/kite_data/another/subdir/file3", "ASSERT")

    RootRepository.registerRoot("EMPTY$", "")
    f("s3n://testkey:secret@data", "TEST_S3N_DATA$")
    f("s3n://testkey:secret@data/uploads/file1", "UPLOAD$/file1")
    f("s3n://testkey:secret@data/uploads/file2", "UPLOAD$/file2")
    f("s3n://testkey:secret@data/uploads/subdir/file3", "UPLOAD$/subdir/file3")
    f("s3n://testkey:secret@data/another/subdir/file3", "TEST_S3N_DATA$/another/subdir/file3")

    f("hdfs:/data", "HDFS$")
    f("hdfs:/data/uploads/file1", "HDFS$/uploads/file1")
    f("hdfs:/data/uploads/file2", "HDFS$/uploads/file2")
    f("hdfs:/data/another/subdir/file3", "HDFS$/another/subdir/file3")

    f("file:/home/user/kite_data", "EMPTY$file:/home/user/kite_data")
    f("file:/home/user/kite_data/uploads/file1", "EMPTY$file:/home/user/kite_data/uploads/file1")
    f("file:/home/user/kite_data/uploads/file2", "EMPTY$file:/home/user/kite_data/uploads/file2")
    f("file:/home/user/kite_data/uploads/subdir/file3", "EMPTY$file:/home/user/kite_data/uploads/subdir/file3")
    f("file:/home/user/kite_data/another/subdir/file3", "EMPTY$file:/home/user/kite_data/another/subdir/file3")
  }

  test("ReadAsString test") {
    val resourceDir = HadoopFile(TestUtils.getDummyRootName(rootPath))
    val text = resourceDir / "multiline.txt"
    assert(text.readAsString() ==
      "Whan that Aprille with his shoures soote\n"
      + "The droghte of Marche hath perced to the roote,\n"
      + "And bathed every veyne in swich licour,\n"
      + "Of which vertu engendred is the flour;\n")
  }
}
