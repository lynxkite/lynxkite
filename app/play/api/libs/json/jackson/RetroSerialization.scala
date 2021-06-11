// https://github.com/playframework/playframework/pull/3784 changed the serialized
// format of Doubles by stripping trailing zeroes. Regrettably, we use the serialized
// format of operation parameters for determining the operation's GUID. To preserve
// compatibility we have a version of the serialization here that does not strip
// trailing zeroes.
//
// This is written against:
// https://github.com/playframework/play-json/blob/2.8.1/play-json/jvm/src/main/scala/play/api/libs/json/jackson/JacksonJson.scala
package play.api.libs.json.jackson

import java.io.InputStream
import java.io.StringWriter

import scala.annotation.switch
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonTokenId
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.`type`.TypeFactory
import com.fasterxml.jackson.databind.deser.Deserializers
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.Serializers

import play.api.libs.json._

// A fork of PlayJsonModule.
sealed class RetroJsonModule(parserSettings: JsonParserSettings)
  extends SimpleModule("RetroJson", Version.unknownVersion()) {
  override def setupModule(context: SetupContext): Unit = {
    context.addSerializers(new RetroSerializers(parserSettings))
  }
}

// A fork of PlaySerializers.
private[jackson] class RetroSerializers(parserSettings: JsonParserSettings)
  extends Serializers.Base {
  override def findSerializer(
    config: SerializationConfig, javaType: JavaType, beanDesc: BeanDescription) = {
    val ser: Object = if (classOf[JsValue].isAssignableFrom(beanDesc.getBeanClass)) {
      new RetroJsValueSerializer(parserSettings)
    } else {
      null
    }
    ser.asInstanceOf[JsonSerializer[Object]]
  }
}

// Monkey patch JsValueSerializer.
private[jackson] class RetroJsValueSerializer(parserSettings: JsonParserSettings)
  extends JsValueSerializer(parserSettings) {
  override def serialize(value: JsValue, json: JsonGenerator, provider: SerializerProvider): Unit = {
    value match {
      // Write it out without stripping trailing zeroes.
      case JsNumber(v) => json.writeNumber(v.bigDecimal)
      case _ => super.serialize(value, json, provider)
    }
  }
}

// Call this on a JsValue to get the compatible ("retro") serialized form.
object RetroSerialization {
  private lazy val mapper =
    (new ObjectMapper).registerModule(new RetroJsonModule(JsonParserSettings.settings))
  private lazy val jsonFactory = new JsonFactory(mapper)
  def apply(j: JsValue): String = {
    val sw = new StringWriter
    val gen = jsonFactory.createGenerator(sw)
    mapper.writeValue(gen, j)
    sw.flush
    sw.getBuffer.toString
  }
}
