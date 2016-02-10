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



split = lynx.newProject()

df = lynx.sqlContext.read()
  .format('com.databricks.spark.csv').option('header', 'true').load(input + '_vertices')
split.importVertices(
  table: lynx.saveAsTable(df, input + '_vertices'),
  "id-attr": 'newId'
)

df = lynx.sqlContext.read()
  .format('com.databricks.spark.csv').option('header', 'true').load(input + '_edges')
split.importEdgesForExistingVertices(
  table: lynx.saveAsTable(df, input + '_edges'),
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

// Create vertex attribute 'originalUniqueId' - this runs between 0 and number of vertices - 1
// Low ids will be treated specially, e.g., splits, and further undefined will come from
// the low regions of the id range.
split.addRandomVertexAttribute(
  name: 'urnd',
  dist: 'Standard Uniform',
  seed: seed
)

split.addRankAttribute(
  rankattr: 'originalUniqueId',
  keyattr: 'urnd',
  order: 'ascending'
)

split.derivedVertexAttribute(
  output: 'split',
  expr: '(originalUniqueId < ' + splits + ') ? 2.0 : 1.0',
  type: 'double'
)

// Save split, because we're going to modify it.
split.copyVertexAttribute(
  from: 'split',
  to: 'splitSave'
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

// Some notation:
// [d-] A vertex whose first attribute is defined, but the second one isn't.
// [-d] A vertex whose second attribute is defined, but the first one isn't.
// [--] A vertex whose both attributes are undefined.
// [dd] A vertex whose both attributes are defined.
//
// We should not let [-d] people call [d-] people and vice versa.
// So, for [-d], we set split to 2.0 and index to 0.
// For [d-], we set split to 2.0 and index to 1.
// This will have the effect that such calls will have
// a splitCalls of 0; and will get deleted subsequently.
split.derivedVertexAttribute(
  output: 'split',
  type: 'double',
  expr: 'furtherUndefinedAttr1 == 1.0 ? 2.0 : split'
)
split.derivedVertexAttribute(
  output: 'split',
  type: 'double',
  expr: 'furtherUndefinedAttr2 == 1.0 ? 2.0 : split'
)
split.derivedVertexAttribute(
  output: 'index',
  type: 'double',
  expr: 'furtherUndefinedAttr2 == 1.0 ? 1.0 : index'
)

// Peripheral vertices have both their attributes undefined to stop them from making it into
// the candidate set. (We'll turn them to [--].)

split.derivedVertexAttribute(
  output: 'attr1',
  expr: '(peripheral == 1.0 || furtherUndefinedAttr1 == 1.0 || (split == 2.0 && index == 0)) ? undefined : originalUniqueId',
  type: 'string'
)

split.derivedVertexAttribute(
  output: 'attr2',
  expr: '(peripheral == 1.0 || furtherUndefinedAttr2 == 1.0 || (split == 2.0 && index == 1)) ? undefined : originalUniqueId',
  type: 'string'
)


split.derivedEdgeAttribute(
  output: 'splitCalls',
  type: 'double',
  expr:
  """
  function Rnd(seedFirst, seedSecond) {
    var seed = util.hash(seedFirst.toString() + '_' + seedSecond.toString());
    var rnd = util.rnd(seed);
    return {
      next: function() {
        return rnd.nextDouble();
      },
    }
  }

  var srcSeed = src\$originalUniqueId
  var dstSeed =  dst\$originalUniqueId
  var srcCount = src\$split;
  var dstCount = dst\$split;
  var srcIdx = src\$index;
  var dstIdx = dst\$index;
  var edgeCnt = originalCalls

  var total = srcCount * dstCount;
  var myId = dstCount * srcIdx + dstIdx;

  function splitCalls() {
    // First, let's consider some cases when it's possible
    // to tell the return value without actually
    // computing edgeCnt random numbers.

    // 0) Pathalogical case: we're invoked from from validateJS
    if (total === 0) {
      return 0;
    }

    // 1) Simplest case: neither the source, nor the destination is
    // split: total === 1 and myId === 0. There is only one
    // edge and it will carry the original count.
    if (total === 1) {
      return edgeCnt;
    }

    // 2) The next simplest case occurs when either the source,
    // or the destination is split, but not both. Here, total === 2
    // and myId falls between 0 and 1 inclusive.
    // We'll need to split edgeCnt between the two edges. However, we cannot
    // avoid generating all edgeCnt random numbers, so the computation
    // must continue.


    // 3) In the most complex case, both the source and the destination
    // vertices are split, resulting in 4 edges. However, we want to
    // devide the edgeCnt quantity between the first edge (myId: 0) and the last
    // one (myId: 3). The two other edges get 0.
    if (total === 4 && (myId === 1 || myId === 2)) {
      return 0;
    }

    var rnd = Rnd(srcSeed, dstSeed)

    var countForTheFirstEdge = 0;
    for (var j = 0; j < edgeCnt; j++) {
      if (rnd.next() < 0.5) {
        countForTheFirstEdge++;
      }
    }
    var countForTheLastEdge = edgeCnt - countForTheFirstEdge;

    var thisIsTheFirstEdge = myId === 0;

    if (thisIsTheFirstEdge) {
      return countForTheFirstEdge;
    } else {
      return countForTheLastEdge;
    }
  }

  splitCalls();
  """
)


split.filterByAttributes(
'filterea-splitCalls': '> 0.0',
)


// Do fingerprinting
split.fingerprintingBasedOnAttributes(
  leftName: 'attr1',
  rightName: 'attr2',
  weights: 'splitCalls',
  mo: '2',
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
  expr: '(peripheral == 0.0 && splitSave == 1.0 && furtherUndefinedAttr1 == 0.0 && furtherUndefinedAttr2 == 0.0) ? 1.0 : 0.0'
)
split.derivedVertexAttribute(
  output: 'pnormal',
  type: 'double',
  expr: '(peripheral == 1.0 && splitSave == 1.0 && furtherUndefinedAttr1 == 0.0 && furtherUndefinedAttr2 == 0.0) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'furtherOk',
  type: 'double',
  expr: '(peripheral == 0.0 && ((furtherUndefinedAttr1 == 1.0 && attr1 == -1) || (furtherUndefinedAttr2 == 1.0 && attr2 == -1))) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'pfurtherOk',
  type: 'double',
  expr: '(peripheral == 1.0 && ((furtherUndefinedAttr1 == 1.0 && attr1 == -1) || (furtherUndefinedAttr2 == 1.0 && attr2 == -1))) ? 1.0 : 0.0'
)


split.derivedVertexAttribute(
  output: 'furtherBad',
  type: 'double',
  expr: '(peripheral == 0.0 && ((furtherUndefinedAttr1 == 1.0 && attr1 != -1) || (furtherUndefinedAttr2 == 1.0 && attr2 != -1))) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'pfurtherBad',
  type: 'double',
  expr: '(peripheral == 1.0 && ((furtherUndefinedAttr1 == 1.0 && attr1 != -1) || (furtherUndefinedAttr2 == 1.0 && attr2 != -1))) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'churnerFound',
  type: 'double',
  expr: '(peripheral == 0.0 && splitSave == 2.0 && attr1 == attr2) ? 1.0 : 0.0'
)
split.derivedVertexAttribute( // This must be 0, see assert at end of file
  output: 'pchurnerFound',
  type: 'double',
  expr: '(peripheral == 1.0 && splitSave == 2.0 && attr1 == attr2 && attr1 != -1) ? 1.0 : 0.0'
)


split.derivedVertexAttribute(
  output: 'churnerNoMatch',
  type: 'double',
  expr: '(peripheral == 0.0 && splitSave == 2.0 && (attr1 == -1 || attr2 == -1)) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'pchurnerNoMatch',
  type: 'double',
  expr: '(peripheral == 1.0 && splitSave == 2.0 && (attr1 == -1 || attr2 == -1)) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'churnerMisMatch', // This must be 0, see assert at end of file
  type: 'double',
  expr: '(peripheral == 0.0 && splitSave == 2.0 && attr1 != -1 && attr2 != -1 && attr2 != attr1) ? 1.0 : 0.0'
)

split.derivedVertexAttribute(
  output: 'pchurnerMisMatch',
  type: 'double',
  expr: '(peripheral == 1.0 && splitSave == 2.0 && attr1 != -1 && attr2 != -1 && attr2 != attr1) ? 1.0 : 0.0'
)


split.derivedVertexAttribute(
  output: 'labelType',
  type: 'string',
  expr:

  """
  var tmp = "";
  tmp += normal == 1.0 ? "normal" : "";
  tmp += furtherOk == 1.0 ? "furtherOk" : "";
  tmp += furtherBad == 1.0 ? "furtherBad" : "";
  tmp += churnerFound == 1.0 ? "churnerFound" : "";
  tmp += churnerNoMatch == 1.0 ? "churnerNoMatch" : "";
  tmp += churnerMisMatch == 1.0 ? "churnerMisMatch" : "";
  tmp;
  """
)


split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-normal": "sum"
)
split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-pnormal": "sum"
)


split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-furtherOk": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-pfurtherOk": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-furtherBad": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-pfurtherBad": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-churnerFound": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-pchurnerFound": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-churnerNoMatch": "sum"
)


split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-pchurnerNoMatch": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-churnerMisMatch": "sum"
)

split.aggregateVertexAttributeGlobally(
  prefix: "",
  "aggregate-pchurnerMisMatch": "sum"
)



vertices=split.scalars['vertex_count'].toString().toDouble()
edges=split.scalars['edge_count'].toString().toDouble()
normal=split.scalars['normal_sum'].toString().toDouble()
furtherOk=split.scalars['furtherOk_sum'].toString().toDouble()
furtherBad=split.scalars['furtherBad_sum'].toString().toDouble()
churnerFound=split.scalars['churnerFound_sum'].toString().toDouble()
churnerNoMatch=split.scalars['churnerNoMatch_sum'].toString().toDouble()
churnerMisMatch=split.scalars['churnerMisMatch_sum'].toString().toDouble()



pnormal = split.scalars['pnormal_sum'].toString().toDouble()
pfurtherOk = split.scalars['pfurtherOk_sum'].toString().toDouble()
pfurtherBad = split.scalars['pfurtherBad_sum'].toString().toDouble()
pchurnerFound = split.scalars['pchurnerFound_sum'].toString().toDouble()
pchurnerNoMatch = split.scalars['pchurnerNoMatch_sum'].toString().toDouble()
pchurnerMisMatch = split.scalars['pchurnerMisMatch_sum'].toString().toDouble()

assert pchurnerMisMatch == 0 : "pchurnerMisMatch should be 0, but it is " + pchurnerMisMatch.toString()
assert pchurnerFound == 0 : "pchurnerFound should be 0, but it is " + pchurnerFound.toString()


System.out.printf("%-20s %15s %15s\n",      "",               'INNER',        'PERIPHERAL');
System.out.printf("%-20s %15.0f %15.0f\n", 'normal',          normal,          pnormal)
System.out.printf("%-20s %15.0f %15.0f\n", 'furtherOk',       furtherOk,       pfurtherOk)
System.out.printf("%-20s %15.0f %15.0f\n", 'furtherBad',      furtherBad,      pfurtherBad)
System.out.printf("%-20s %15.0f %15.0f\n", 'churnerFound',    churnerFound,    pchurnerFound)
System.out.printf("%-20s %15.0f %15.0f\n", 'churnerNoMatch',  churnerNoMatch,  pchurnerNoMatch)
System.out.printf("%-20s %15.0f %15.0f\n", 'churnerMisMatch', churnerMisMatch, pchurnerMisMatch)
System.out.printf("\n");
System.out.printf("%-20s %.0f\n", 'edges', edges)
System.out.printf("%-20s %.0f\n", 'vertices', vertices)
