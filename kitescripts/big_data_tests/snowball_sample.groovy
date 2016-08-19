// Tests the "Snowball sample" FE operation

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.snowballSample(
        ratio: '0.001',
        radius: '5',
        attrName: 'distance_from_start_point',
        seed: '123454321'
)

project.computeUncomputed()
