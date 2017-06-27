// Generates a random graph of a given size.
project.newVertexSet(size: params['size'])
project.createScaleFreeRandomEdgeBundle(
  iterations: 5,
  perIterationMultiplier: 1.6,
  seed: 0)

// Do something conditioned on a parameter value.
if (params['degree'] != '') {
  // Double quotes (") can be used for embedding parameters in strings.
  project.degree(direction: params['degree'], name: "count of ${params['degree']}")

  // The keys can be constructed similarly.
  // For aggregations you can just list the attribute you need.
  project.aggregateOnNeighbors(
    "aggregate_count of ${params['degree']}": 'max',
    direction: 'incoming edges',
    prefix: 'neighborhood')

  // You can create saved visualizations from workflows:
  project.saveUIStatusAsGraphAttribute(
    scalarName: 'Everything',
    uiStatusJson: """{
      "filters": {
        "edge": {},
        "vertex": {}
      },
      "axisOptions": {
        "edge": {},
        "vertex": {}
      },
      "graphMode": "sampled",
      "bucketCount": "4",
      "sampleRadius": "1",
      "display": "svg",
      "animate": {
        "enabled": true,
        "labelAttraction": "0",
        "style": "neutral"
      },
      "attributeTitles": {
        "size": "count of ${params['degree']}",
        "color": "neighborhood_count of ${params['degree']}_max"
      },
      "centers": [
        "*"
      ]
    }""")
}
