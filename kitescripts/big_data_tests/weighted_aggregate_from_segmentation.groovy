// Tests the "Weighted aggregate from segmentation" operation

/// REQUIRE_SCRIPT segment_by_interval.groovy


project = lynx.loadProject('segment_by_interval_result')
segmentation = project.segmentations['seg_interval']

segmentation.weightedAggregateFromSegmentation(
  'weight': 'size',
  'prefix': '',
  'aggregate-top': 'weighted_sum'
)

histogram = project.vertexAttributes['top_weighted_sum_by_size'].histogram(
  logarithmic: true,
  precise: true)

println "Weighted aggregate to segmentation histogram: $histogram"

