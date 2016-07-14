// Test "Segment by interval"

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject("random_attributes")

project.renameVertexAttribute(
  from: 'rnd_std_normal',
  to: 'i_begin')

project.derivedVertexAttribute(
  output: 'i_end',
  type: 'double',
  expr: 'i_begin + Math.abs(rnd_std_normal2)')

project.segmentByInterval(
  'begin_attr': 'i_begin',
  'end_attr': 'i_end',
  'interval_size': '0.01',
  name: 'seg_interval',
  overlap: 'no')

project.segmentByInterval(
  'begin_attr': 'i_begin',
  'end_attr': 'i_end',
  'interval_size': '0.01',
  name: 'seg_interval_overlap',
  overlap: 'yes')

project.computeUncomputed()
project.saveAs('segment_by_interval_result')
