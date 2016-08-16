// Test basic segmentation creation operations.

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject("random_attributes")

project.segmentByDoubleAttribute(
  name: 'seg',
  attr: 'rnd_std_normal',
  'interval-size': '0.001',
  overlap: 'no')
s = project.segmentations['seg']
println "[seg] vertices: ${s.scalars['vertex_count']} edges: ${s.scalars['edge_count']}"

project.segmentByDoubleAttribute(
  name: 'seg_overlap',
  attr: 'rnd_std_normal',
  'interval-size': '0.001',
  overlap: 'yes')
s = project.segmentations['seg_overlap']
println "[seg_overlap] vertices: ${s.scalars['vertex_count']} edges: ${s.scalars['edge_count']}"

project.derivedVertexAttribute(
  output: 'x',
  type: 'string',
  expr: 'rnd_std_uniform.toFixed(7)')
project.segmentByStringAttribute(
  name: 'seg_string',
  attr: 'x')
project.computeUncomputed()
project.saveAs("segmentations")
