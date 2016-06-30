// Tests the "Define segmentation links from matching attributes" operation


/// REQUIRE_SCRIPT create_segmentation_from_sql.groovy

name = 'create_segmentation_from_sql_result'

project = lynx.loadProject(name)
project.vertexAttributeToDouble(
  'attr': 'ordinal'
)
project.derivedVertexAttribute(
  'output': 'src',
  'type': 'string',
  'expr': 'Math.floor(ordinal/3.0)'
)


segmentation = project.segmentations['segmentation']
segmentation.vertexAttributeToDouble(
  'attr': 'ordinal'
)
segmentation.derivedVertexAttribute(
  'output': 'dst',
  'type': 'string',
  'expr': 'Math.floor(ordinal/5.0)'
)

segmentation.defineSegmentationLinksFromMatchingAttributes(
  'base-id-attr': 'src',
  'seg-id-attr': 'dst',
)


df = segmentation.sql("select * from belongs_to")
lynx.saveAsTable(df, "segmentation_belongs_to_table")

println "belongsTo Edges: ${segmentation.scalars['!belongsToEdges']}"

