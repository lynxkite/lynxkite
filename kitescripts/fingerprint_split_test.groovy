// The input graph is expected to be a sort of 'callgraph'.
// It should contain neither loop edges nor multiple edges.
// It should have a vertex attribute 'peripheral' which is 0
// when the vertex has all its neighbors in the graph, and 1
// when the vertex (in the original graph) had some neighbors
// which are not present in the input graph.
// Is should also have an edge attribute 'originalCalls', which is the number of times
// one vertex (user) called another one. This property is used as a weight for the finger
// printing algorithm.


// Parameters
furtherUndefinedAttr1 = params.fa1 ?: '5'
furtherUndefinedAttr2 = params.fa2 ?: '5'
splitProb = params.splitProb ?: '0.3'
splits = params.splits ?: '10'
input =  params.input ?: 'fprandom'
seed = params.seed ?: '31415'


furtherUndefinedAttr1Expr =
        '(originalUniqueId >= ' +
        splits +
        ' && ' +
        splits +
        ' + ' +
        furtherUndefinedAttr1 +
        ' > originalUniqueId) ? 1.0 : 0.0'


furtherUndefinedAttr2Expr =
        '(originalUniqueId >= ' +
        splits +
        ' + ' +
        furtherUndefinedAttr1 +
        ' && ' +
        splits +
        ' + ' +
        furtherUndefinedAttr1 +
        ' + ' +
        furtherUndefinedAttr2 +
        ' > originalUniqueId) ? 1.0 : 0.0'



jsprogram=
"""
function Hasher() {
  var hash = function(data) {
    var h = 0x34140;
    var phi = 0x9e377; // 2^20 * phi   (sqrt(5)-1)/2
    for (var i = 0; i < data.length; i++) {
      h = h ^ data.charCodeAt(i);
      h = h * phi;
      h = h & 0xfffff;
    }
    return h;
  }
  return hash;
}

function Rnd(seedFirst, seedSecond) {
  var hasher = Hasher();
  var seed = hasher(seedFirst.toString() + '_' + seedSecond.toString());
  var unifRand = function() {
    var a = Math.sin(seed++) * 10000;
    var b = a - Math.floor(a);
    return b;
  }
  return {
    random: unifRand,
    geomChoose: function(p, lastId) {      
      for (var i = 0; i <= lastId; i++) {
          var q = unifRand();
          if (q < p) return i;
      }
      return lastId;
    },
  }
};



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
var lastId = total - 1;

(function() {
  if (total === 1) {
    return edgeCnt;
  }

  var randomFunc = Rnd(srcSeed, dstSeed).geomChoose
  
  var count = 0;

  for (var j = 0; j < edgeCnt; j++) {    
    if (randomFunc(prob, lastId) === myId) count++;
  }

  return count;
})();
"""

split=lynx.newProject('split test for FP')
split.importVerticesFromCSVFiles(
  files: 'DATA$exports/' + input + '_vertices/data/part*',
  header: '"id","peripheral"',
  delimiter: ',',
  omitted: '',
  filter: '',
  "id-attr": 'newId',
  allow_corrupt_lines: 'no'
)
split.importEdgesForExistingVerticesFromCSVFiles(
  files: 'DATA$exports/' + input + '_edges/data/part*',
  header: '"src_id","dst_id","originalCalls"',
  delimiter: ',',
  omitted: '',
  filter: '',
  allow_corrupt_lines: 'no',
  attr: 'id',
  src: 'src_id',
  dst: 'dst_id'  
)
// Convert strings to doubles:
split.vertexAttributeToDouble(
  attr: 'peripheral'
)
split.edgeAttributeToDouble(
  attr: 'originalCalls'
)

// Create vertex attribute 'originalUniqueId' - this runs beteen 0 and number of vertices - 1
// Low ids will be treated specially, e.g., splits, and further undefined will come from
// the low regions of the id range. We don't want peripheral vertices to be treated
// specially, so we make sure that they are assigned higher ids.
split.addRandomVertexAttribute(
  name: 'urnd',
  dist: 'Standard Uniform',
  seed: seed
)

split.derivedVertexAttribute(
  output: 'urndPeripheralHigh',
  expr: 'peripheral == 0.0 ? urnd : urnd + 2.0',
  type: 'double'
)

split.addRankAttribute(
  rankattr: 'originalUniqueId',
  keyattr: 'urndPeripheralHigh',
  order: 'ascending'
) 

split.derivedVertexAttribute(
  output: 'split',
  expr: '(originalUniqueId < ' + splits + ') ? 2.0 : 1.0',
  type: 'double'
)

split.derivedVertexAttribute(
  output: 'furtherUndefinedAttr1',
  type: 'double',
  expr: furtherUndefinedAttr1Expr
)

split.derivedVertexAttribute(
  output: 'furtherUndefinedAttr2',
  type: 'double',
  expr: furtherUndefinedAttr2Expr
)

split.vertexAttributeToString(
  attr: 'originalUniqueId'
)

split.splitVertices(
  rep: 'split',
  idattr: 'newId',
  idx: 'index'
)
split.vertexAttributeToDouble(
  attr: 'index'
)

split.derivedVertexAttribute(
  output: 'attr1',
  expr: '(furtherUndefinedAttr1 == 1.0 || (split == 2.0 && index == 0)) ? undefined : originalUniqueId',
  type: 'string'
)

split.derivedVertexAttribute(
  output: 'attr2',
  expr: '(furtherUndefinedAttr2 == 1.0 || (split == 2.0 && index == 1)) ? undefined : originalUniqueId',
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
  extra: '"weightingMode": "InDegree", "multiNeighborsPreference": 5.0, "alpha": -1.0',
  ms: '0.0'
)


split.fillWithConstantDefaultValue(
  attr: 'attr1',
  def: '-1'
)

split.fillWithConstantDefaultValue(
  attr: 'attr2',
  def: '-1'
)


split.derivedVertexAttribute(
  output: 'label',
  type: 'string',
  expr: 'originalUniqueId + "," + attr1 + "," + attr2'
)


split.derivedVertexAttribute(
  output: 'normal',
  type: 'double',
  expr: '(split == 1.0 && furtherUndefinedAttr1 == 0.0 && furtherUndefinedAttr2 == 0.0) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'furtherOk',
  type: 'double',
  expr: '((furtherUndefinedAttr1 == 1.0 && attr1 == -1) || (furtherUndefinedAttr2 == 1.0 && attr2 == -1)) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'furtherBad',
  type: 'double',
  expr: '((furtherUndefinedAttr1 == 1.0 && attr1 != -1) || (furtherUndefinedAttr2 == 1.0 && attr2 != -1)) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'churnerFound',
  type: 'double',
  expr: '(split == 2.0 && attr1 == attr2) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'churnerNoMatch',
  type: 'double',
  expr: '(split == 2.0 && (attr1 == -1 || attr2 == -1)) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'churnerMisMatch',
  type: 'double',
  expr: '(split == 2.0 && attr1 != -1 && attr2 != -1 && attr2 != attr1) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'labelType',
  type: 'string',
  expr:

  """
  (function(){
  var tmp = "";
  tmp += normal == 1.0 ? "normal" : "";
  tmp += furtherOk == 1.0 ? "furtherOk" : "";
  tmp += furtherBad == 1.0 ? "furtherBad" : "";
  tmp += churnerFound == 1.0 ? "churnerFound" : "";
  tmp += churnerNoMatch == 1.0 ? "churnerNoMatch" : "";
  tmp += churnerMisMatch == 1.0 ? "churnerMisMatch" : "";
  return tmp;
  })();
  """
)


split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-normal": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-furtherOk": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-furtherBad": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-churnerFound": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-churnerNoMatch": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-churnerMisMatch": "sum"
)

vertices=split.scalars['vertex_count']
edges=split.scalars['edge_count']
normal=split.scalars['normal_sum']
furtherOk=split.scalars['furtherOk_sum']
furtherBad=split.scalars['furtherBad_sum']
churnerFound =split.scalars['churnerFound_sum']
churnerNoMatch =split.scalars['churnerNoMatch_sum']
churnerMisMatch =split.scalars['churnerMisMatch_sum']

println "vertices: $vertices"
println "edges: $edges"

println "normal $normal"
println "furtherOk $furtherOk"
println "furtherBad $furtherBad"
println "churnerFound $churnerFound"
println "churnerNoMatch $churnerNoMatch"
println "churnerMisMatch $churnerMisMatch"
