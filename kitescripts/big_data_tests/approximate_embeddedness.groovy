// Tests the "Approximate embeddedness" operation.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.approximate_embeddedness(
        'name': 'embeddedness',
        'bits': '12',
)

project.saveAs('approximate_embeddedness_result')

project.computeUncomputed()
