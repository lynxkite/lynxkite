// A convenient interface for running operations.
//
// It is used everywhere where we want to run an operation. It relies on having
// access to an implicit MetaGraphManager (for metagraph operations) or DataManager
// (for data access operations).
//
// Usage:
//
//   import Scripting._
//
//   def getSomething(
//     input1: VertexSet,
//     input2: Attribute[Double])(
//       implicit m: MetaGraphManager): Attribute[String] = {
//     val op = graph_operations.MyOperation(1, 2, 3)
//     op(op.input1, input1)(op.input2, input2).result.output1
//   }
//
//   def getScalarValue(scalar: Scalar[T])(implicit m: DataManager): T = {
//     scalar.value
//   }

package com.lynxanalytics.biggraph.graph_api

object Scripting {
  import scala.language.implicitConversions

  implicit class InstanceBuilder[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
      val op: TypedMetaGraphOp[IS, OMDS]) {
    val builder = this
    private var currentInput = MetaDataSet()
    def apply[T <: MetaGraphEntity](
      adder: EntityTemplate[T],
      container: EntityContainer[T]): InstanceBuilder[IS, OMDS] = apply(adder, container.entity)
    def apply[T <: MetaGraphEntity](
      adder: EntityTemplate[T],
      entity: T): InstanceBuilder[IS, OMDS] = {
      currentInput = adder.set(currentInput, entity)
      this
    }
    def apply[T <: MetaGraphEntity](
      adders: Seq[EntityTemplate[T]],
      entities: Seq[T]): InstanceBuilder[IS, OMDS] = {
      assert(adders.size == entities.size, {
        val adderNames = adders.map(_.name)
        s"Input sequence mismatch: $adderNames vs $entities"
      })
      for ((adder, entity) <- adders.zip(entities)) {
        currentInput = adder.set(currentInput, entity)
      }
      this
    }

    def apply() = this

    def toInstance(manager: MetaGraphManager): TypedOperationInstance[IS, OMDS] = {
      manager.apply(op, currentInput)
    }
  }

  implicit def buildInstance[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    builder: InstanceBuilder[IS, OMDS])(
      implicit manager: MetaGraphManager): TypedOperationInstance[IS, OMDS] =
    builder.toInstance(manager)

  implicit def getData(entity: EntityContainer[VertexSet])(
    implicit dataManager: DataManager): VertexSetData =
    dataManager.get(entity.entity)
  implicit def getData(entity: EntityContainer[EdgeBundle])(
    implicit dataManager: DataManager): EdgeBundleData =
    dataManager.get(entity.entity)
  implicit def getData[T](entity: EntityContainer[Attribute[T]])(
    implicit dataManager: DataManager): AttributeData[T] =
    dataManager.get(entity.entity)
  implicit def getData[T](entity: EntityContainer[Scalar[T]])(
    implicit dataManager: DataManager): ScalarData[T] =
    dataManager.get(entity.entity)
  implicit def getData(entity: EntityContainer[Table])(
    implicit dataManager: DataManager): TableData =
    dataManager.get(entity.entity)

  implicit def getData(entity: VertexSet)(
    implicit dataManager: DataManager): VertexSetData =
    dataManager.get(entity)
  implicit def getData(entity: EdgeBundle)(
    implicit dataManager: DataManager): EdgeBundleData =
    dataManager.get(entity)
  implicit def getData[T](entity: Attribute[T])(
    implicit dataManager: DataManager): AttributeData[T] =
    dataManager.get(entity)
  implicit def getData[T](entity: Scalar[T])(
    implicit dataManager: DataManager): ScalarData[T] =
    dataManager.get(entity)
  implicit def getData(entity: Table)(
    implicit dataManager: DataManager): TableData =
    dataManager.get(entity)

  implicit def toInput[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    op: TypedMetaGraphOp[IS, OMDS]): IS = op.inputs

  implicit def emptyInputInstance[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    op: TypedMetaGraphOp[IS, OMDS])(
      implicit manager: MetaGraphManager): TypedOperationInstance[IS, OMDS] =
    manager.apply(op, MetaDataSet())
}
