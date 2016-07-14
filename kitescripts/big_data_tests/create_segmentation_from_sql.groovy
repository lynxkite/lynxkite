// Tests the "Create segmentation from SQL" operation

/// REQUIRE_SCRIPT random_attributes.groovy

name = 'random_attributes'

project = lynx.loadProject(name)

project.createSegmentationFromSQL(
  'name': 'segmentation',
  'sql': 'select * from vertices'
)

s = project.segmentations['segmentation']

println "vertices: ${s.scalars['vertex_count']}"

project.saveAs('create_segmentation_from_sql_result')
