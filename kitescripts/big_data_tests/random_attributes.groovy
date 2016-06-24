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
project.edgeAttributes['rnd_std_uniform']
  .computeAndPrintHistogram(name: 'rnd_std_uniform (edges)')

project.addRandomVertexAttribute(
  name: 'rnd_std_uniform',
  dist: 'Standard Uniform',
  seed: '12342')
project.vertexAttributes['rnd_std_uniform']
  .computeAndPrintHistogram(name: 'rnd_std_uniform (vertices)')

project.addRandomEdgeAttribute(
  name: 'rnd_std_normal',
  dist: 'Standard Normal',
  seed: '12343')
project.edgeAttributes['rnd_std_normal']
  .computeAndPrintHistogram(name: 'rnd_std_normal (edges)')

project.addRandomVertexAttribute(
  name: 'rnd_std_normal',
  dist: 'Standard Normal',
  seed: '12344')
project.vertexAttributes['rnd_std_normal']
  .computeAndPrintHistogram(name: 'rnd_std_normal (vertices)')


project.addRandomVertexAttribute(
  name: 'rnd_std_normal2',
  dist: 'Standard Normal',
  seed: '12345')
project.vertexAttributes['rnd_std_normal2']
        .computeAndPrintHistogram(name: 'rnd_std_normal2 (vertices)')

project.saveAs('random_attributes')
