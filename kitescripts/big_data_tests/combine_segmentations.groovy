// Test combine segmentations.

/// REQUIRE_SCRIPT segmentations.groovy

project = lynx.loadProject("segmentations")

project.combineSegmentations(
  name: 'seg_combined',
  segmentations: 'seg,seg_overlap,seg_string')

project.computeUncomputed()

