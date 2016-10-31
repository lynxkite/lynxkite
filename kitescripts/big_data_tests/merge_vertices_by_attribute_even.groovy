// Tests the "Merge vertices by attribute" operation for long tail distribution.

project = lynx.newProject()
project.newVertexSet(size: 500000000)
project.addRandomVertexAttribute(
  name: 'rnd_std_uniform',
  dist: 'Standard Uniform',
  seed: '12344')

project.derivedVertexAttribute(
  'output': 'label',
  'type': 'double',
  'expr': 'Math.floor(rnd_std_uniform*5000000)'
)

project.mergeVerticesByAttribute(
  'key': 'label',
  'aggregate-rnd_std_uniform': 'average'
)

project.computeUncomputed()
