// Tests the "Embeddedness" operation.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.embeddedness(
        'name': 'embeddedness',
)

project.saveAs('embeddedness_result')

project.computeUncomputed()
