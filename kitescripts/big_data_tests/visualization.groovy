// Tests graph visualization.

/// REQUIRE_SCRIPT filter_high_degree_vertices.groovy

seed = 12321

project = lynx.loadProject('filter_high_degree_vertices_result')

project.addRandomVertexAttribute(
  name: 'randomV',
  dist: 'Standard Uniform',
  seed: seed + 2
)
project.addRandomVertexAttribute(
  name: 'randomV2',
  dist: 'Standard Uniform',
  seed: seed + 3
)
project.addRandomEdgeAttribute(
  name: 'randomE',
  dist: 'Standard Uniform',
  seed: seed + 4
)

project.segmentByDoubleAttribute(
  name: 'segmentation',
  attr: 'randomV',
  'interval-size': '0.01',
  overlap: 'no'
)
segmentation = project.segmentations['segmentation']
segmentation.createRandomEdgeBundle(degree: 10.0, seed: seed + 5)

def testComplexView(title, project, request) {
  println title
  result = project.getComplexView(request)
  println "  vertex sets:"
  i = 0
  for (vs in scala.collection.JavaConversions.seqAsJavaList(result.vertexSets)) {
    println "    ${i}: size= ${vs.size()}"
    ++i
  }
  println "  edge bundles:"
  i = 0
  for (es in scala.collection.JavaConversions.seqAsJavaList(result.edgeBundles)) {
    println "    ${i}: size= ${es.size()}"
    ++i
  }
}

testComplexView(
  "1: visualize one graph",
  project,
  lynx.drawing.newFEGraphRequest(
    vertexSets: [
      lynx.drawing.newVertexDiagramSpec(
        vertexSetId: project.getVertexSetId(),
        sampleSmearEdgeBundleId: project.getEdgeBundleId(),
        mode: "sampled",
        centralVertexIds: project.getCenters(3),
      )
    ],
    edgeBundles: [
      lynx.drawing.newEdgeDiagramSpec(
        srcIdx: 0,
        dstIdx: 0,
        edgeBundleId: project.getEdgeBundleId()
      )
    ]
  )
)

testComplexView(
  "2. Visualize one graph with filters",
  project,
  lynx.drawing.newFEGraphRequest(
    vertexSets: [
      lynx.drawing.newVertexDiagramSpec(
        vertexSetId: project.getVertexSetId(),
        sampleSmearEdgeBundleId: project.getEdgeBundleId(),
        mode: "sampled",
        centralVertexIds: project.getCenters(3),
        filters: [
          lynx.drawing.newVertexAttributeFilter(
            attributeId: project.vertexAttributes['randomV'].id,
            valueSpec: "> 0.2"),
        ],
        attrs: [
          project.vertexAttributes['randomV'].id
        ]
      )
    ],
    edgeBundles: [
      lynx.drawing.newEdgeDiagramSpec(
        srcIdx: 0,
        dstIdx: 0,
        edgeBundleId: project.getEdgeBundleId()
      )
    ]
  )
)

testComplexView(
  "3. Get visualization of a graph plus a segmentation",
  project,
  lynx.drawing.newFEGraphRequest(
    vertexSets: [
      lynx.drawing.newVertexDiagramSpec(
        vertexSetId: project.getVertexSetId(),
        sampleSmearEdgeBundleId: project.getEdgeBundleId(),
        mode: "sampled",
        centralVertexIds: project.getCenters(3),
      ),
      lynx.drawing.newVertexDiagramSpec(
        vertexSetId: segmentation.getVertexSetId(),
        sampleSmearEdgeBundleId: segmentation.getEdgeBundleId(),
        mode: "sampled",
        centralVertexIds: segmentation.getCenters(3),
      ),
    ],
    edgeBundles: [
      lynx.drawing.newEdgeDiagramSpec(
        srcIdx: 0,
        dstIdx: 0,
        edgeBundleId: project.getEdgeBundleId()
      ),
      lynx.drawing.newEdgeDiagramSpec(
        srcIdx: 1,
        dstIdx: 1,
        edgeBundleId: segmentation.getEdgeBundleId()
      ),
      lynx.drawing.newEdgeDiagramSpec(
        srcIdx: 0,
        dstIdx: 1,
        edgeBundleId: segmentation.getBelongsToId()
      ),
    ]
  )
)

testComplexView(
  "4. Bucketed view",
  project,
  lynx.drawing.newFEGraphRequest(
    vertexSets: [
      lynx.drawing.newVertexDiagramSpec(
        vertexSetId: project.getVertexSetId(),
        sampleSmearEdgeBundleId: project.getEdgeBundleId(),
        mode: "bucketed",
        xBucketingAttributeId:
          project.vertexAttributes['randomV'].id,
        yBucketingAttributeId:
          project.vertexAttributes['randomV2'].id,
        xNumBuckets: 10,
        yNumBuckets: 10
      )
    ],
    edgeBundles: [
      lynx.drawing.newEdgeDiagramSpec(
        srcIdx: 0,
        dstIdx: 0,
        edgeBundleId: project.getEdgeBundleId()
      )
    ]
  )
)

