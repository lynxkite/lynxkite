package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.TestUtils
import org.scalatest.funsuite.AnyFunSuite

class HadoopFileTest extends AnyFunSuite {
  val prefixPath = getClass.getResource("/graph_util/hadoop_tests").toString
  PrefixRepository.registerPrefix("HADOOPTEST$", prefixPath)

  ignore("Test Hadoop file system cache") {
    val p = "FILESYSTEMCACHEEMPTY$"
    PrefixRepository.registerPrefix(p, "")
    val testCache = new HadoopFileSystemCache(5000L)

    val local = testCache.fs(HadoopFile(s"${p}file:/tmp1/home"))
    val hdfs1 = testCache.fs(HadoopFile(s"${p}hdfs://localhost:9000/user/kite"))
    val hdfs2 = testCache.fs(HadoopFile(s"${p}HDFS://localhost:9000/user/lynx"))
    val s3_1 = testCache.fs(HadoopFile(s"${p}s3://key:pwd@localhost:8000/data"))
    val s3_2 = testCache.fs(HadoopFile(s"${p}s3://lynx-network-test"))

    assert(local != null)
    assert(hdfs1 != null)
    assert(hdfs2 != null)
    assert(s3_1 != null)
    assert(s3_2 != null)

    assert(!(s3_1 eq s3_2))
    assert(!(local eq hdfs1))
    assert(!(local eq s3_1))
    assert(!(hdfs1 eq s3_1))
    assert(hdfs1 eq hdfs2)
    assert(local eq testCache.fs(HadoopFile(s"${p}file:/tmp1/home")))

    Thread.sleep(10000L)
    val hdfs3 = testCache.fs(HadoopFile(s"${p}hdfs://localhost:9000/user/lynx"))
    assert(hdfs3 != null)
    assert(!(hdfs1 eq hdfs3))
    assert(!(local eq testCache.fs(HadoopFile(s"${p}file:/tmp1/home"))))
  }

  test("Test basic PrefixRepository asserts") {
    PrefixRepository.registerPrefix("BABABA$", "x:mamam")
    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("BABABA$", "x:mamam")
    }
    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("KJHKJSDDSJ@", "x:mamam")
    }
    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("KJHKJSDDSJ$/haha", "x:mamam")
    }
    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("NOSCHEME$", "noschemetobefoundhere/alma")
    }
    PrefixRepository.registerPrefix("_$", "")
    PrefixRepository.registerPrefix("AB_$", "")
    PrefixRepository.registerPrefix("A1$", "")
    PrefixRepository.registerPrefix("QQ$", "")
    PrefixRepository.registerPrefix("Q012$", "")
    PrefixRepository.registerPrefix("P4Q1$", "")
    PrefixRepository.registerPrefix("W$", "")

    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("9$", "")
    }
    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("3P4$", "")
    }
    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("$", "")
    }
    intercept[java.lang.AssertionError] {
      PrefixRepository.registerPrefix("A$B", "")
    }
  }

  test("Password setting works - s3n") {
    val dummy = TestUtils.getDummyPrefixName("s3n://access:secret@lynx-bnw-test2")
    val dataFile = HadoopFile(dummy + "/somedir/somefile")
    val conf = dataFile.hadoopConfiguration()
    assert(conf.get("fs.s3n.awsAccessKeyId") == "access")
    assert(conf.get("fs.s3n.awsSecretAccessKey") == "secret")
  }

  test("Password setting works - s3") {
    val dummy = TestUtils.getDummyPrefixName("s3://access:secret@lynx-bnw-test2")
    val dataFile = HadoopFile(dummy + "/somedir/somefile")
    val conf = dataFile.hadoopConfiguration()
    assert(conf.get("fs.s3.awsAccessKeyId") == "access")
    assert(conf.get("fs.s3.awsSecretAccessKey") == "secret")
  }

  test("Password setting works - s3a") {
    val dummy = TestUtils.getDummyPrefixName("s3a://access:secret@lynx-bnw-test2")
    val dataFile = HadoopFile(dummy + "/somedir/somefile")
    val conf = dataFile.hadoopConfiguration()
    assert(conf.get("fs.s3a.access.key") == "access")
    assert(conf.get("fs.s3a.secret.key") == "secret")
  }

  test("Path concatenation works") {
    val dummy = TestUtils.getDummyPrefixName("s3n://access:secret@lynx-bnw-test2")
    val d = HadoopFile(dummy) / "dir/file"
    assert(d.resolvedNameWithNoCredentials == "s3n://lynx-bnw-test2/dir/file")
    val q = d + ".ext"
    assert(q.resolvedNameWithNoCredentials == "s3n://lynx-bnw-test2/dir/file.ext")
  }

  def wildcardTest(resourcePrefix: HadoopFile) = {
    val all = resourcePrefix / "*"
    assert(all.list.length == 5)
    val txt = resourcePrefix / "*.txt"
    assert(txt.list.length == 3)
  }

  test("Wildcard matching works") {
    wildcardTest(HadoopFile("HADOOPTEST$"))
  }

  test("Hadoop normalization works for operator /") {
    PrefixRepository.registerPrefix("ROOT1$", "file:/home/rootdir/")
    val f1 = HadoopFile("ROOT1$") / "file.txt"
    assert(f1.symbolicName == "ROOT1$file.txt")
    assert(f1.resolvedName == "file:/home/rootdir/file.txt")

    PrefixRepository.registerPrefix("ROOT2$", "file:/home/rootdir")
    val f2 = HadoopFile("ROOT2$") / "file.txt"
    assert(f2.symbolicName == "ROOT2$/file.txt")
    assert(f2.resolvedName == "file:/home/rootdir/file.txt")

  }

  test("Hadoop forward-backward conversion works") {

    PrefixRepository.registerPrefix("HADOOPROOTA$", "file:/home/rootdir")
    val f1 = HadoopFile("HADOOPROOTA$/subdir") / "*"
    val g1 = f1.hadoopFileForGlobOutput("file:/home/rootdir/subdir/file")
    assert(g1.symbolicName == "HADOOPROOTA$/subdir/file")

    PrefixRepository.registerPrefix("HADOOPROOTB$", "s3n://key:secret@rootdir")
    val f2 = HadoopFile("HADOOPROOTB$/subdir") / "*"
    val g2 = f2.hadoopFileForGlobOutput("s3n://rootdir/subdir/file")
    assert(g2.symbolicName == "HADOOPROOTB$/subdir/file")

    PrefixRepository.registerPrefix("HADOOPROOTC$", "s3n:/")
    val f3 = HadoopFile("HADOOPROOTC$/key:secret@rootdir/subdir1/file")
    assert(f3.normalizedRelativePath == "/key:secret@rootdir/subdir1/file")
    val g3 = f3.hadoopFileForGlobOutput("s3n://rootdir/subdir1/file")
    assert(g3.awsId == "key")
    assert(g3.awsSecret == "secret")

    PrefixRepository.registerPrefix("HADOOPROOTD$", "s3n://")
    val f4 = HadoopFile("HADOOPROOTD$key:secret@rootdir/subdir1/file")
    assert(f4.normalizedRelativePath == "key:secret@rootdir/subdir1/file")
    val g4 = f4.hadoopFileForGlobOutput("s3n://rootdir/subdir1/file")
    assert(g4.awsId == "key")
    assert(g4.awsSecret == "secret")

    PrefixRepository.registerPrefix("HADOOP_ROOT$", "s3n:")
    val f5 = HadoopFile("HADOOP_ROOT$//key:secret@rootdir/subdir1/file")
    assert(f5.normalizedRelativePath == "//key:secret@rootdir/subdir1/file")
    val g5 = f5.hadoopFileForGlobOutput("s3n://rootdir/subdir1/file")
    assert(g5.awsId == "key")
    assert(g5.awsSecret == "secret")

    PrefixRepository.registerPrefix("HADOOP_ROOT1$", "file:/home///user/")
    val f6 = HadoopFile("HADOOP_ROOT1$file.txt")
    assert(f6.normalizedRelativePath == "file.txt")
    val g6 = f6.hadoopFileForGlobOutput("file:/home/user/file.txt")
    assert(g6.symbolicName == "HADOOP_ROOT1$file.txt")

    PrefixRepository.registerPrefix("HADOOP_ROOT2$", "file:/home///user/")
    val f7 = HadoopFile("HADOOP_ROOT2$/file.txt")
    assert(f7.normalizedRelativePath == "file.txt")
    val g7 = f7.hadoopFileForGlobOutput("file:/home/user/file.txt")
    assert(g7.symbolicName == "HADOOP_ROOT2$file.txt")

  }

  test("Empty symbolic prefix works with file:// scheme") {
    PrefixRepository.registerPrefix("EMPTYFILE$", "")
    val resourceDir = HadoopFile("EMPTYFILE$") + prefixPath
    wildcardTest(resourceDir)
  }

  def checkOne(prefixSymbol: String, pathAndOutput: Tuple2[String, String]) = {
    val (relativePath, expectedOutput) = (pathAndOutput._1, pathAndOutput._2)
    if (expectedOutput == "ASSERT") {
      intercept[java.lang.AssertionError] {
        HadoopFile(prefixSymbol + relativePath)
      }
    } else {
      val file = HadoopFile(prefixSymbol + relativePath)
      assert(file.resolvedName == expectedOutput)
    }
  }

  def checkPathRules(
      prefixResolution: String,
      relativePathsAndExpectedOutputs: List[Tuple2[String, String]]) = {
    val prefixSymbol = TestUtils.getDummyPrefixName(prefixResolution, false)
    relativePathsAndExpectedOutputs.foreach { checkOne(prefixSymbol, _) }
  }

  test("Dangerous concatenations get caught") {

    checkPathRules(
      "",
      List(
        ("a", "a"),
        ("/haha", "/haha"),
        ("///g///", "/g/"),
        ("", ""),
        ("b///", "b/"),
        ("/user/../trick", "ASSERT")))

    checkPathRules(
      "x:b",
      List(
        ("a", "ASSERT"),
        ("/haha", "x:b/haha"),
        ("///g///", "x:b/g/"),
        ("", "x:b"),
        ("b///", "ASSERT"),
        ("/user/../trick", "ASSERT")))

    checkPathRules(
      "x:b/",
      List(
        ("a", "x:b/a"),
        ("/haha", "x:b/haha"),
        ("///g///", "x:b/g/"),
        ("", "x:b/"),
        ("b///", "x:b/b/"),
        ("/user/../trick", "ASSERT")))

    checkPathRules(
      "s3n://key:secret@",
      List(
        ("a", "s3n://key:secret@a"),
        ("///b", "s3n://key:secret@b"),
        ("/hello..", "ASSERT")))

    checkPathRules(
      "x:alma.",
      List(
        ("a", "ASSERT"),
        ("///b", "x:alma./b"),
        (".trick", "ASSERT")))

    checkPathRules(
      "file:/home",
      List(
        ("/user", "file:/home/user"),
        ("//user", "file:/home/user"),
        ("user", "ASSERT")))

    checkPathRules(
      "x:/home",
      List(
        ("/user", "x:/home/user"),
        ("//user", "x:/home/user"),
        ("user", "ASSERT")))

  }

  test("Check user defined path parsing") {
    val inputLines = """# Comment
              |#
              |
              |# Blank
              |
              |#COMMENTEDOUT="file:/home/user/"
              |
              |# # #
              |#
              |TEST_EMPTY="" #empty
              |
              |TEST_S3N="s3n://testkey:testpwd@" #
              |
              |TEST_S3NDIR="s3n://testkey:testpwd@directory/" #####
              |
              |TESTFILEDIR="file:/home/user/"
              |
              |          # This doesn't end in a slash!!!
              |          TESTBLANKS_="hdfs://root/path"
              |
              | TESTFILEDIR_READ_ACL="*"
              | TESTFILEDIR_WRITE_ACL="gabor.olah@lynxanalytics.com"
              |# Only whitespace
              |
              |
              |
              |
              |
              |
              |""".stripMargin('|').split("\n").toList

    val pairs = PrefixRepositoryImpl.parseInput(inputLines)
    val expected = List(
      "TEST_EMPTY" -> "",
      "TEST_S3N" -> "s3n://testkey:testpwd@",
      "TEST_S3NDIR" -> "s3n://testkey:testpwd@directory/",
      "TESTFILEDIR" -> "file:/home/user/",
      "TESTFILEDIR_READ_ACL" -> "*",
      "TESTFILEDIR_WRITE_ACL" -> "gabor.olah@lynxanalytics.com",
      "TESTBLANKS_" -> "hdfs://root/path",
    )
    assert(pairs.sorted === expected.sorted)
  }

  test("Check user defined path: parsing blanks at the end of line") {
    val pairs = PrefixRepositoryImpl.parseInput(
      List(
        "PATH=\"hdfs://pathnode/\"  ",
        "PATH2=\"hdfs://pathnode2/\"\t\t \t"))
    val expected = List(
      "PATH" -> "hdfs://pathnode/",
      "PATH2" -> "hdfs://pathnode2/")
    assert(pairs.sorted === expected.sorted)
  }

  test("ACLs can be retrieved") {
    val input =
      """
        |PATH1="hdfs://node1/"
        |
        |PATH2="hdfs://node2/"
        |PATH2_READ_ACL="*@lynx1"
        |PATH2_WRITE_ACL="*@lynx2"
      """.stripMargin.split("\n").toList
    val prefixRepo = new PrefixRepositoryImpl(input, false)

    // No settings: default
    assert(prefixRepo.getReadACL("PATH1$") == "*")
    assert(prefixRepo.getWriteACL("PATH1$") == "*")

    // Given settings: retrieved correctly
    assert(prefixRepo.getReadACL("PATH2$") == "*@lynx1")
    assert(prefixRepo.getWriteACL("PATH2$") == "*@lynx2")
  }

  test("Lopsided settings cause an assert") {
    val input =
      """
        |PATH="hdfs://node2/"
        |PATH_READ_ACL="*@lynx"
      """.stripMargin.split("\n").toList
    intercept[Throwable] {
      new PrefixRepositoryImpl(input, false)
    }
  }

  test("Misspelled settings cause an assert") {
    val input =
      """
        |PAHT="hdfs://node2/"
        |PATH_READ_ACL="*@lynx"
        |PATH_WRITE_ACL="*@lynx"
      """.stripMargin.split("\n").toList
    intercept[Throwable] {
      new PrefixRepositoryImpl(input, false)
    }
  }

  test("ReadAsString test") {
    val resourceDir = HadoopFile(TestUtils.getDummyPrefixName(prefixPath))
    val text = resourceDir / "multiline.txt"
    assert(text.readAsString() ==
      "Whan that Aprille with his shoures soote\n"
      + "The droghte of Marche hath perced to the roote,\n"
      + "And bathed every veyne in swich licour,\n"
      + "Of which vertu engendred is the flour;\n")
  }

  test("We can allow absolute paths") {
    val p = new PrefixRepositoryImpl(List(), true)
    p.registerPrefix("HOME$", "file://home/user")
    assert(Tuple2("HOME$", "/dir") == p.splitSymbolicPattern("HOME$/dir"))
    assert(Tuple2("", "file:/home/another") == p.splitSymbolicPattern("file:/home/another"))
  }
}
