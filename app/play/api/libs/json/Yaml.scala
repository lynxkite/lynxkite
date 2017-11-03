// Utility for parsing YAML into JsValue. Unfortunately the JacksonJson object in Play is package
// private, so we have to infiltrate the package to be able to use the same mechanism with YAML.
package play.api.libs.json

import com.fasterxml.jackson

object Yaml {
  val mapper = (new jackson.databind.ObjectMapper).registerModule(JacksonJson.module)
  val jsonFactory = new jackson.dataformat.yaml.YAMLFactory(mapper)

  def parseJsValue(input: String): JsValue = {
    val parser = jsonFactory.createParser(input)
    mapper.readValue(parser, classOf[JsValue])
  }
}
