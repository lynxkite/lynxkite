// Applies the PBKDF2 algorithm to mask an attribute: https://en.wikipedia.org/wiki/PBKDF2
// The salt is given by the user and used to defend against attacks using a list of pre-computed hashes for the
// probable values (rainbow table attacks).
// The algorithm can made quicker or slower by setting the number iterations. Higher speed also means that attacker
// can make guesses for cheaper.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import javax.crypto.spec.PBEKeySpec

object SecretKeyFactory {
  val secretKeyFactory: javax.crypto.SecretKeyFactory = javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
}

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

  // The preferred length of the hash. The length was choosen according to the probabilities listed in
  // https://en.wikipedia.org/wiki/Birthday_problem#Probability_table
  val keyLength = 96
  val iterations = 1

  val secretKeyFactory = SecretKeyFactory.secretKeyFactory

  // Using the javax.crypto library to create the hash function
  def hash(string: String, salt: String) = {
    val spec: PBEKeySpec = new PBEKeySpec(string.toCharArray, salt.getBytes, iterations, keyLength)
    val hashedPassword: Array[Byte] = secretKeyFactory.generateSecret(spec).getEncoded
    hashedPassword.map("%02X".format(_)).mkString
  }
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
      "Secret string should be protected with SECRET(...)")
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
    implicit val runtimeContext = rc

    val hashed = inputs.attr.rdd.mapValues(v => HashVertexAttribute.hash(v, salt))
    output(o.hashed, hashed)
  }
}
