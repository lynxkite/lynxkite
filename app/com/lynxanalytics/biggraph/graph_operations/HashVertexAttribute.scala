// Applies the PBKDF2 algorithm to mask an attribute: https://en.wikipedia.org/wiki/PBKDF2
// The salt is given by the user and used to defend against attacks using a list of pre-computed hashes for the
// probable values (rainbow table attacks).
// The algorithm can made quicker or slower by setting the number iterations. Higher speed also means that attacker
// can make guesses for cheaper.

package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api._
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import java.math.BigInteger

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
}
import HashVertexAttribute._
case class HashVertexAttribute(salt: String)
    extends TypedMetaGraphOp[Input, Output] {
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

    // The preferred length of the hash.
    val keyLength = 80
    val iterations = 5

    // Using the javax.crypto library to create the hash function
    def hash(string: String) = {
      val spec: PBEKeySpec = new PBEKeySpec(string.toCharArray, salt.getBytes, iterations, keyLength)
      val key: SecretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
      val hashedPassword: Array[Byte] = key.generateSecret(spec).getEncoded
      hashedPassword.map("%02X" format _).mkString
      //     String.format("%x", new BigInteger(hashedPassword))
    }

    output(o.hashed, inputs.attr.rdd.mapValues(v => hash(v)))
  }
}
