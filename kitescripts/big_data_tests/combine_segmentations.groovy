// Test combine segmentations.

/// REQUIRE_SCRIPT segmentations.groovy

project = lynx.loadProject("segmentations")

project.combineSegmentations(
  name: 'seg_combined',
  segmentations: 'seg,seg_overlap,seg_string')

s = project.segmentations['seg_combined']
println "[seg_combined] vertices: ${s.scalars['vertex_count']} edges ${s.scalars['edge_count']}"
