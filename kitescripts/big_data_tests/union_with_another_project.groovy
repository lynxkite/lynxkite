// Tests the "Union with another project" operation
// We're unioning with ourselves

/// REQUIRE_SCRIPT edge_import.groovy

name = 'edge_import_result'

project = lynx.loadProject(name)
otherName = project.rootCheckpointWithTitle(name)

project.unionWithAnotherProject(
  'other': otherName,
  'id-attr': 'new_id'
)


project.computeUncomputed()


