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

project.addRandomVertexAttribute(
  name: 'rnd_std_uniform',
  dist: 'Standard Uniform',
  seed: '12342')

project.addRandomEdgeAttribute(
  name: 'rnd_std_normal',
  dist: 'Standard Normal',
  seed: '12343')

project.addRandomVertexAttribute(
  name: 'rnd_std_normal',
  dist: 'Standard Normal',
  seed: '12344')


project.addRandomVertexAttribute(
  name: 'rnd_std_normal2',
  dist: 'Standard Normal',
  seed: '12345')

project.addRankAttribute(
  'rankattr': 'ordinal',
  'keyattr': 'rnd_std_uniform',
  'order': 'ascending'
)

project.computeUncomputed()
project.saveAs('random_attributes')
