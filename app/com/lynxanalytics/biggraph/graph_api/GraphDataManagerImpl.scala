package com.lynxanalytics.biggraph.graph_api

private[graph_api] class GraphDataManagerImpl(repositoryPath: String)
    extends GraphDataManager {
  def obtainData(bigGraph: BigGraph): GraphData = ???

  def saveDataToDisk(bigGraph: BigGraph) = ???

  def runtimeContext: RuntimeContext = ???
}
