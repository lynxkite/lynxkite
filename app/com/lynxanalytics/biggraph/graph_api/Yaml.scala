// Utility for parsing YAML into JsValue.
package com.lynxanalytics.biggraph.graph_api

import com.fasterxml.jackson
import play.api.libs.json

object Yaml {
  val mapper = (new jackson.databind.ObjectMapper).registerModule(
    new json.jackson.PlayJsonModule(json.JsonParserSettings()))
  val jsonFactory = new jackson.dataformat.yaml.YAMLFactory(mapper)

  def parseJsValue(input: String): json.JsValue = {
    val parser = jsonFactory.createParser(input)
    mapper.readValue(parser, classOf[json.JsValue])
  }
}
