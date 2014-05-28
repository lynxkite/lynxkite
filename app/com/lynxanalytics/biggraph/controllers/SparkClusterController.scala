package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnviroment
import com.lynxanalytics.biggraph.serving

case class SparkClusterStatusResponse(
  master: String,
  workerInstances: Int)

case class SetClusterNumInstanceRequest(
  password: String,
  workerInstances: Int)

class SparkClusterController(enviroment: BigGraphEnviroment) {
  def getClusterStatus(request: serving.Empty): SparkClusterStatusResponse = {
    SparkClusterStatusResponse(enviroment.sparkContext.master, enviroment.numInstances)
  }

  def setClusterNumInstances(request: SetClusterNumInstanceRequest): SparkClusterStatusResponse = {
    if (request.password != "UCU8HB0d6fQJwyD8UAdDb")
      throw new IllegalArgumentException("Bad password!")
    enviroment.setNumInstances(request.workerInstances)
    return getClusterStatus(serving.Empty())
  }
}
