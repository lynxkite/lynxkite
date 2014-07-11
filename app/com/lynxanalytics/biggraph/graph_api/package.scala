package com.lynxanalytics.biggraph

import org.apache.spark.rdd

package object graph_api {
  type ID = Long

  type VertexSetRDD = rdd.RDD[(ID, Unit)]

  type AttributeRDD[T] = rdd.RDD[(ID, T)]

  case class Edge(val src: ID, val dst: ID)
  type EdgeBundleRDD = rdd.RDD[(ID, Edge)]

  import scala.language.implicitConversions
  implicit def unpackTemplate[T <: MetaGraphEntity](
    template: EntityTemplate[T])(
      implicit instance: MetaGraphOperationInstance): T = template.entity

  implicit def unpackContainer[T <: MetaGraphEntity](container: EntityContainer[T]): T =
    container.entity

  implicit class InstanceBuilder[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
      op: TypedMetaGraphOp[IS, OMDS]) {
    private var currentInput = MetaDataSet()
    def set[T <: MetaGraphEntity](adder: EntityTemplate[T], container: EntityContainer[T]): InstanceBuilder[IS, OMDS] = set(adder, container.entity)
    def set[T <: MetaGraphEntity](adder: EntityTemplate[T], entity: T): InstanceBuilder[IS, OMDS] = {
      currentInput = adder.set(currentInput, entity)
      this
    }

    def toInstance(manager: MetaGraphManager): TypedOperationInstance[IS, OMDS] = {
      manager.apply(op, currentInput)
    }
  }

  implicit def buildInstance[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    builder: InstanceBuilder[IS, OMDS])(implicit manager: MetaGraphManager): TypedOperationInstance[IS, OMDS] = builder.toInstance(manager)

  implicit def getData(entity: EntityContainer[VertexSet])(
    implicit dataManager: DataManager): VertexSetData =
    dataManager.get(entity)
  implicit def getData(entity: EntityContainer[EdgeBundle])(
    implicit dataManager: DataManager): EdgeBundleData =
    dataManager.get(entity)
  implicit def getData[T](entity: EntityContainer[VertexAttribute[T]])(
    implicit dataManager: DataManager): VertexAttributeData[T] =
    dataManager.get(entity)
  implicit def getData[T](entity: EntityContainer[EdgeAttribute[T]])(
    implicit dataManager: DataManager): EdgeAttributeData[T] =
    dataManager.get(entity)
  implicit def getData[T](entity: EntityContainer[Scalar[T]])(
    implicit dataManager: DataManager): ScalarData[T] =
    dataManager.get(entity)

  implicit def toInput[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    op: TypedMetaGraphOp[IS, OMDS]): IS = op.inputs

  implicit def emptyInputInstance[IS <: InputSignatureProvider, OMDS <: MetaDataSetProvider](
    op: TypedMetaGraphOp[IS, OMDS])(
      implicit manager: MetaGraphManager): TypedOperationInstance[IS, OMDS] =
    manager.apply(op, MetaDataSet())

}
