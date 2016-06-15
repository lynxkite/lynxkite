// Test random attribute generation.

// The main use of this script is to provide input for other tests.
// It looks a little silly that we import graphs from S3 just so that
// we put random attributes on them.

/// REQUIRE_SCRIPT edge_import.groovy

project = lynx.loadProject('edge_import_result')

project.addRandomEdgeAttribute(
  name: 'rnd_std_uniform',
  dist: 'Standard Uniform',
  seed: '12341')
println "rnd_std_uniform (edges): ${ project.edgeAttributes['rnd_std_uniform'].histogram(precise: true) }"

project.addRandomVertexAttribute(
  name: 'rnd_std_uniform',
  dist: 'Standard Uniform',
  seed: '12342')
println "rnd_std_uniform (vertices): ${ project.vertexAttributes['rnd_std_uniform'].histogram(precise: true) }"

project.addRandomEdgeAttribute(
  name: 'rnd_std_normal',
  dist: 'Standard Normal',
  seed: '12343')
println "rnd_std_normal (edges): ${ project.edgeAttributes['rnd_std_normal'].histogram(precise: true) }"

project.addRandomVertexAttribute(
  name: 'rnd_std_normal',
  dist: 'Standard Normal',
  seed: '12344')
println "rnd_std_normal (vertices): ${ project.vertexAttributes['rnd_std_normal'].histogram(precise: true) }"

project.addRandomVertexAttribute(
  name: 'rnd_std_normal2',
  dist: 'Standard Normal',
  seed: '12345')
println "rnd_std_normal2 (vertices): ${ project.vertexAttributes['rnd_std_normal2'].histogram(precise: true) }"

project.saveAs('random_attributes')
