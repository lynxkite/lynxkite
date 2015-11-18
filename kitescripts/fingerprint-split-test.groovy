// Parameters
vertices='1000'
ebSize='10'
splits='200'
seed='31415'
mostCallsPossible='20'
splitProb='0.25'


jsprogram=
"""
// Xorshift 128
// This is ideal, because we have 128 bits to initialize the seed
// (2 random ids, 64 bits each)
// Well, sort of. The double vs long issue can interfere here, but
// I don't think it's terribly important.

var s_x = 1;
var s_y = 2;
var s_z = 3;
var s_w = 4;

function rndxor()
{
  var t = s_x ^ (s_x << 11);
  s_x = s_y;
  s_y = s_z;
  s_z = s_w;
  s_w = s_w ^ (s_w >> 19) ^ t ^ (t>>8);
  return s_w / 2147483648.0;
}
function setseed(s1, s2) {
  var two_to_the_32 = 4294967296.0;
  var mask = two_to_the_32 - 1;

  // Only 63 bits are actually used.
  var ss1 = s1 < 0 ? -s1 : s1;
  var ss2 = s2 < 0 ? -s2 : s2;
  
  var hi1 = ss1 / two_to_the_32;
  var hi2 = ss2 / two_to_the_32;
  
  var lo1 = ss1 & mask;
  var lo2 = ss2 & mask;
  
  s_x = hi1;
  s_y = hi2;
  s_z = lo1;
  s_z = lo2;

  for (var i = 0; i < 10; i++)
    rndxor();
}

var srcSeed = src\$originalUniqueId
var dstSeed =  dst\$originalUniqueId
var srcCount = src\$split;
var dstCount = dst\$split;
var srcIdx = src\$index;
var dstIdx = dst\$index;
var edgeCnt = originalCalls
var prob = $splitProb

var total = srcCount * dstCount;
var myId = dstCount * srcIdx + dstIdx;

function geom_choose(n,p) {
  for (var k = 0; k < n - 1; k++) {
    var q = rndxor();
    if (q < p) return k;
  }
  return n - 1;
}


function compute()
{
  if (total == 1) {
    return edgeCnt;
  }

  
  setseed(srcSeed,dstSeed);

  
  var counts = [];
  for (var i = 0; i < total; i++)
  {
    counts.push(0);
  }

  for (var j = 0; j < edgeCnt; j++) {
    var k = geom_choose(total, prob);
    counts[k]++;
  }

  return counts[myId];
}


compute();


"""


split=lynx.newProject('split test for FP')
split.newVertexSet(size: vertices)
split.createRandomEdgeBundle(degree: ebSize, seed: seed)
split.discardLoopEdges()
split.mergeParallelEdges()

split.addGaussianVertexAttribute(name: 'random', seed: seed)
split.addRankAttribute(keyattr: 'random', order: 'ascending', rankattr: 'originalUniqueId')
split.derivedVertexAttribute(
  output: 'split',
  expr: '(originalUniqueId < ' + splits + ') ? 2.0 : 1.0',
  type: 'double'
)
split.vertexAttributeToString(
  attr: 'originalUniqueId'
)

split.derivedEdgeAttribute(
  output: 'originalCalls',
  expr: 'Math.floor(Math.random() * ' + mostCallsPossible + ');',
  type: double
)
split.splitVertices(
  rep: 'split',
  idattr: 'newId',
  idx: 'index'
)

split.derivedVertexAttribute(
  output: 'attr1',
  expr: '(split == 1.0 || index == 0) ? originalUniqueId : undefined',
  type: 'string'
)

split.derivedVertexAttribute(
  output: 'attr2',
  expr: '(split == 1.0 || index == 1) ? originalUniqueId : undefined',
  type: 'string'
)

split.derivedEdgeAttribute(
  output: 'splitCalls',
  type: double,
  expr: jsprogram
)

split.filterByAttributes('filterea-splitCalls': '> 0.0')


// Do fingerprinting
split.fingerprintingBasedOnAttributes(
  leftName: 'attr1',
  rightName: 'attr2',
  weights: 'splitCalls',
  mo: '2',
  ms: '0.0',
)

// Evaluate
split.derivedVertexAttribute(
  output: 'leftResult',
  type: 'double',
  expr: 'attr1 == originalUniqueId ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'rightResult',
  type: 'double',
  expr: 'attr2 == originalUniqueId ? 1.0 : 0.0'
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-leftResult": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-rightResult": "sum"
)


leftRes = split.scalars['leftResult_sum']
rightRes = split.scalars['rightResult_sum']


println "leftResult: $leftRes"
println "rightResult: $rightRes"
vertices=split.scalars['vertex_count']
edges=split.scalars['edge_count']
println "vertices: $vertices"
println "edges: $edges"
