// Tests the "Create snowball sample" FE operation

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.createSnowballSample(
        ratio: '0.0001',
        radius: '1',
        attrName: 'distance_from_start_point',
        seed: '123454321'
)

project.computeUncomputed()
