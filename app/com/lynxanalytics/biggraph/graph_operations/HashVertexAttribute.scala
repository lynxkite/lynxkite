// Applies a cryptographic hash function to an attribute.
// The salt is given by the user and used to defend against attacks using a list of pre-computed
// hashes for the probable values (rainbow table attacks) and against known-plaintext attacks.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._

object HashVertexAttribute extends OpFromJson {
  class Input extends MagicInputSignature {
    val vs = vertexSet
    val attr = vertexAttribute[String](vs)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val hashed = vertexAttribute[String](inputs.vs.entity)
  }
  def fromJson(j: JsValue) = HashVertexAttribute(
    (j \ "salt").as[String])

  val algorithm = new ThreadLocal[java.security.MessageDigest] {
    override def initialValue() = java.security.MessageDigest.getInstance("SHA-256")
  }

  def hash(string: String, salt: String) = {
    val hashedPassword: Array[Byte] = algorithm.get().digest((string + salt).getBytes("utf8"))
    hashedPassword.map("%02X".format(_)).mkString
  }

  // "Secret" mechanism, used for holding back sensitive strings from logging.
  def makeSecret(stringToHide: String) = {
    val ret = "SECRET(" + stringToHide + ")"
    assertSecret(ret)
    ret
  }
  def getSecret(secretString: String): String = {
    secretString.stripPrefix("SECRET(").stripSuffix(")")
  }
  def assertSecret(str: String): Unit = {
    assert(str.startsWith("SECRET(") && str.endsWith(")"),
      "Secret string should be protected. Use SECRET( followed by your secret string, followed by a closing bracket")
    assert(!getSecret(str).contains(")"),
      "Secret string should not contain a closing bracket")
  }
}
import HashVertexAttribute._
case class HashVertexAttribute(salt: String)
    extends TypedMetaGraphOp[Input, Output] {
  assertSecret(salt)
  override val isHeavy = true
  @transient override lazy val inputs = new Input()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)
  override def toJson = Json.obj("salt" -> salt)
  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas

    val hashed = inputs.attr.rdd.mapValues(v => HashVertexAttribute.hash(v, salt))
    output(o.hashed, hashed)
  }
}
