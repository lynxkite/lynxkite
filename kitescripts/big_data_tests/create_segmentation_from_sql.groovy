// Tests the "Create segmentation from SQL" operation

/// REQUIRE_SCRIPT edge_import.groovy

name = 'edge_import_result'

project = lynx.loadProject(name)

project.createSegmentationFromSQL(
  'name': 'segmentation',
  'sql': 'select * from vertices'
)

s = project.segmentations['segmentation']

println "vertices: ${s.scalars['vertex_count']}"

