package com.lynxanalytics.biggraph.graph_api

import java.util.UUID

abstract class MetaGraphManager {
  def deriveGraph(sources: Seq[BigGraph],
                  operation: GraphOperation): BigGraph

  def graphForGUID(gUID: UUID): BigGraph

  def knownDerivates(graph: BigGraph): Seq[BigGraph]
}
