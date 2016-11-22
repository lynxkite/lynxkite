// Tests the "Merge vertices by attribute" operation for long tail distribution.

project = lynx.newProject()
project.newVertexSet(size: 500000000)
project.addRandomVertexAttribute(
  name: 'rnd_std_normal',
  dist: 'Standard Normal',
  seed: '12344')

project.derivedVertexAttribute(
  'output': 'label',
  'type': 'double',
  'expr': 'Math.floor(Math.exp(rnd_std_normal))'
)

project.mergeVerticesByAttribute(
  'key': 'label',
  'aggregate-rnd_std_normal': 'average'
)

project.computeUncomputed()
