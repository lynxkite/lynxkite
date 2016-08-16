// Tests the "Triadic Closure" FE operation

/// REQUIRE_SCRIPT random_attributes.groovy
// Random attributes are used for testing the pulling of the edge attributes

project = lynx.loadProject('random_attributes')

project.triadicClosure()

project.computeUncomputed()
