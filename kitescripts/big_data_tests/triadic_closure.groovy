// Tests the "Triadic Closure" FE operation

/// REQUIRE_SCRIPT random_attributes.groovy

project = lynx.loadProject('random_attributes')

project.triadicClosure()

project.computeUncomputed()