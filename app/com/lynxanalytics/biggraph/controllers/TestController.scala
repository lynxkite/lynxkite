package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.graph_api
import com.lynxanalytics.biggraph.serving

/**
 * Case classes used by the JsonServer to communicate with the web application
 */

case class TestRequest(attr: String)
case class TestResponse(attr: String)

/**
 * Logic for processing requests
 */

object TestController {
  def process(request: TestRequest): TestResponse = {
    TestResponse("test string: " + request.attr)
  }
}