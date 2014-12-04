package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import play.api.test.FakeRequest
import play.api.test.FakeHeaders
import play.api.test.Helpers
import play.api.test.Helpers._
import play.api.libs.json.Json
import com.lynxanalytics.biggraph.serving.JsonServer

/* play.api.test should be replaced with https://github.com/scalatest/scalatestplus-play
 * as soon as it is published with documentation. Should happen any day.
 * More information: https://groups.google.com/forum/#!topic/scalatest-users/u7LKrKcV1k
 */

case class TestRequest(attr: String)
case class TestResponse(attr: String)

class TestController {
  def process(user: securesocial.core.Identity, request: TestRequest): TestResponse = {
    TestResponse("test string: " + request.attr)
  }
}

object TestJsonServer extends JsonServer {
  implicit val rTest = Json.reads[TestRequest]
  implicit val wTest = Json.writes[TestResponse]

  val testController = new TestController
  def testPost = jsonPost(testController.process)
  def testGet = jsonGet(testController.process)
}

class JsonTest extends FunSuite {
  test("call testPost with a valid fake POST message") {
    val jsonString = """{"attr":"Hello BigGraph!"}"""
    val request = FakeRequest(
      POST,
      "/api/test",
      FakeHeaders(Seq("Content-Type" -> Seq("application/json"))),
      Json.parse(jsonString))
    val result = TestJsonServer.testPost(request)
    assert(Helpers.status(result) === OK)
    assert((Json.parse(Helpers.contentAsString(result)) \ ("attr")).toString
      === "\"test string: Hello BigGraph!\"")
  }

  test("call testGet with a valid fake GET message") {
    val jsonString = """{"attr":"Hello BigGraph!"}"""
    val request = FakeRequest(GET, "/api/test?q=" + jsonString)
    val result = TestJsonServer.testGet(request)
    assert(Helpers.status(result) === OK)
    assert((Json.parse(Helpers.contentAsString(result)) \ ("attr")).toString
      === "\"test string: Hello BigGraph!\"")
  }

  test("testPost should raise exception if JSON is incorrect") {
    val jsonString = """{"bad attr":"Hello BigGraph!"}"""
    val request = FakeRequest(
      POST,
      "/api/test",
      FakeHeaders(Seq("Content-Type" -> Seq("application/json"))),
      Json.parse(jsonString))
    intercept[Throwable] {
      TestJsonServer.testPost(request)
    }
  }

  test("testGet should raise exception if JSON is incorrect") {
    val jsonString = """{"bad attr":"Hello BigGraph!"}"""
    val request = FakeRequest(GET, "/api/test?q=" + jsonString)
    intercept[Throwable] {
      TestJsonServer.testGet(request)
    }
  }

  test("testGet should raise exception if query parameter is incorrect") {
    val jsonString = """{"attr":"Hello BigGraph!"}"""
    val request = FakeRequest(GET, "/api/test?gugu=" + jsonString)
    intercept[Throwable] {
      TestJsonServer.testGet(request)
    }
  }
}
