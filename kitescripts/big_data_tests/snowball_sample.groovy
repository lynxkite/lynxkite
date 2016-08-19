// Tests the "Snowball sample" FE operation

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.snowballSample()

project.computeUncomputed()
