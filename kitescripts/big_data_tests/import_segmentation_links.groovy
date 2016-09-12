// Tests the "Import segmentation links" operation


/// REQUIRE_SCRIPT define_segmentation_links_from_matching_attributes.groovy

name = 'create_segmentation_from_sql_result'

project = lynx.loadProject(name)
segmentation = project.segmentations['segmentation']

segmentation.importSegmentationLinks(
  'table': lynx.openTable('segmentation_belongs_to_table'),
  'base-id-attr': 'ordinal',
  'base-id-column': 'base_ordinal',
  'seg-id-attr': 'ordinal',
  'seg-id-column': 'segment_ordinal'
)

project.computeUncomputed()


