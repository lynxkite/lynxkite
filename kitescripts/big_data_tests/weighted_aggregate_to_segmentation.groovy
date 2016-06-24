// Tests the "Weighted aggregate to segmentation" operation

/// REQUIRE_SCRIPT segment_by_interval.groovy


project = lynx.loadProject('segment_by_interval_result')
segmentation = project.segmentations['seg_interval']

segmentation.weightedAggregateToSegmentation(
  'weight': 'rnd_std_uniform',
  'aggregate-rnd_std_normal': 'weighted_sum'
)

segmentation
  .vertexAttributes['rnd_std_normal_weighted_sum_by_rnd_std_uniform']
  .computeAndPrintHistogram(
    name: 'eighted aggregate to segmentation histogram',
    logarithmic: true,
  )


