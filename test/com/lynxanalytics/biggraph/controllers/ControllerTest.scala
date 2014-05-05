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


class ControllerTest extends FunSuite {
  test("call testPost with a fake POST message") {
    val jsonString = """{"attr":"Hello BigGraph!"}"""
    val request = FakeRequest(POST,
                              "/api/test",
                              FakeHeaders(Seq("Content-Type" -> Seq("application/json"))),
                              Json.parse(jsonString))
    val result = JsonServer.testPost(request)
    assert(Helpers.status(result) === OK)
    assert((Json.parse(Helpers.contentAsString(result)) \ ("attr")).toString
        === "\"POST test string: Hello BigGraph!\"")
  }
}