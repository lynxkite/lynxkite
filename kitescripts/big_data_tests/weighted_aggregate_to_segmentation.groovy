// Tests the "Weighted aggregate to segmentation" operation

/// REQUIRE_SCRIPT segment_by_interval.groovy


project = lynx.loadProject('segment_by_interval_result')
segmentation = project.segmentations['seg_interval']

segmentation.weightedAggregateToSegmentation(
  'weight': 'rnd_std_uniform',
  'aggregate-rnd_std_normal2': 'weighted_sum'
)

project.computeUncomputed()
