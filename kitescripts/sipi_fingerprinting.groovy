// Before running this script, untar this archive into the uploads dir:
//  https://drive.google.com/a/lynxanalytics.com/file/d/0B4_SBhzYAJK8VnJqLUNJdnoyalk/view?usp=sharing

// ========= Preparing the LinkedIn graph ==================
linkedin = lynx.newProject('linkedin for FP')

// Import linkedin vertices
linkedin.importVerticesFromCSVFiles(
  allow_corrupt_lines: 'no',
  delimiter: ',',
  files: 'UPLOAD$/linkedin_vertices.csv',
  filter: '',
  header: '<read first line>',
  'id-attr': 'id',
  omitted: '')

// Import linkedin edges
linkedin.importEdgesForExistingVerticesFromCSVFiles(
  allow_corrupt_lines: 'no',
  attr: 'linkedin_id',
  delimiter: ',',
  src: 'Source',
  dst: 'Target',
  files: 'UPLOAD$/linkedin_edges.csv',
  filter: '',
  header: '<read first line>',
  omitted: '')
linkedin.discardEdgeAttribute(name: 'Source')
linkedin.discardEdgeAttribute(name: 'Target')
linkedin.addReversedEdges()

// Remove dupe names.
linkedin.filterByAttributes('filterva-linkedin_name': '!Attila Szabó,Damokos László,Gábor Bóna,Mátyás Krizsák,Peter Varga')

// Load edit distance matched facebook names.
linkedin.importVertexAttributesFromCSVFiles(
  allow_corrupt_lines: 'no',
  delimiter: ',',
  files: 'UPLOAD$/facebook_linkedin_name_pairs.csv',
  filter: '',
  header: 'matched_fb_name,matched_linkedin_name',
  'id-attr': 'linkedin_name',
  'id-field': 'matched_linkedin_name',
  omitted: '',
  prefix: '')

// Somewhat hacky way to split to test/train sets.
linkedin.addGaussianVertexAttribute(name: 'random', seed: '0')
linkedin.addRankAttribute(keyattr: 'random', order: 'ascending', rankattr: 'ranking')
linkedin.derivedVertexAttribute(
  output: 'train_matched_fb_name',
  expr: '(ranking < 162) ? undefined : matched_fb_name',
  type: 'string')
linkedin.derivedVertexAttribute(
  output: 'test_matched_fb_name',
  expr: '(ranking < 162) ? matched_fb_name : undefined',
  type: 'string')

// Create key that we will use to merge with FB vertices.
linkedin.copyVertexAttribute(from: 'random', to: 'random_str')
linkedin.vertexAttributeToString(attr: 'random_str')
linkedin.mergeTwoAttributes(
  'attr1': 'train_matched_fb_name',
  'attr2': 'random_str',
  name: 'merge_key_name')


// ========= Preparing the FaceBook graph ==================
fb = lynx.newProject('facebook for FP')
fb.importVerticesFromCSVFiles(
  allow_corrupt_lines: 'no',
  delimiter: ',',
  files: 'UPLOAD$/facebook_vertices.csv',
  filter: '',
  header: '<read first line>',
  'id-attr': 'id',
  omitted: '')
fb.importEdgesForExistingVerticesFromCSVFiles(
  allow_corrupt_lines: 'no',
  attr: 'fb_id',
  delimiter: ',',
  src: 'node_from',
  dst: 'node_to',
  files: 'UPLOAD$/facebook_edges.csv',
  filter: '',
  header: '<read first line>',
  omitted: '')
fb.discardEdgeAttribute(name: 'node_from')
fb.discardEdgeAttribute(name: 'node_to')
fb.addReversedEdges()
fb.copyVertexAttribute(from: 'fb_name', to: 'merge_key_name')

// ================== Take the union, merge and fingerprint =======================
union = linkedin.saveAs('linkedin facebook FP')
union.unionWithAnotherProject('id-attr': 'new_id', other: 'facebook for FP')

union.mergeVerticesByAttribute(
  key: 'merge_key_name',
  'aggregate-fb_id': 'most_common',
  'aggregate-fb_name': 'most_common',
  'aggregate-id': 'count',
  'aggregate-linkedin_id': 'most_common',
  'aggregate-linkedin_name': 'most_common',
  'aggregate-matched_fb_name': 'most_common',
  'aggregate-train_matched_fb_name': 'most_common',
  'aggregate-test_matched_fb_name': 'most_common')

// Remove stupid most_common suffixes.
union.renameVertexAttribute(from: 'fb_id_most_common', to: 'fb_id')
union.renameVertexAttribute(from: 'fb_name_most_common', to: 'fb_name')
union.renameVertexAttribute(from: 'linkedin_id_most_common', to: 'linkedin_id')
union.renameVertexAttribute(from: 'linkedin_name_most_common', to: 'linkedin_name')
union.renameVertexAttribute(from: 'matched_fb_name_most_common', to: 'matched_fb_name')
union.renameVertexAttribute(from: 'train_matched_fb_name_most_common', to: 'train_matched_fb_name')
union.renameVertexAttribute(from: 'test_matched_fb_name_most_common', to: 'test_matched_fb_name')

// Fingerprint.
union.derivedEdgeAttribute(expr: 'Math.min(src$id_count + dst$id_count - 1, 2)', output: 'weight', type: 'double')
union.fingerprintingBasedOnAttributes(
  leftName: 'fb_name',
  rightName: 'linkedin_name',
  mo: '1',
  ms: '0.0',
  weights: '!no weight',
  extra: '"weightingMode": "InverseInDegreeBasedHybrid", "multiNeighborsPreference": 5.0')

// Evaluate.
union.derivedVertexAttribute(
  expr: '(test_matched_fb_name === fb_name) ? 1.0 : 0.0',
  output: 'match_is_good',
  type: 'double')
union.aggregateVertexAttributeGlobally(
  'aggregate-match_is_good': 'sum,count',
  'aggregate-test_matched_fb_name': 'count',
  prefix: '')

union.renameScalar(from: 'test_matched_fb_name_count', to: 'test_set_size')
union.renameScalar(from: 'match_is_good_count', to: 'matched_count')
union.renameScalar(from: 'match_is_good_sum', to: 'correct_match_count')

testSetSize = union.scalars['test_set_size'].toDouble()
println "Test set size: $testSetSize"

// ======= Drawing a primitive P/R curve ============

pr = union.saveAs('linkedin facebook PR curve')

// First restrict to matched test vertices. This is basically an is-defined test.
pr.filterByAttributes('filterva-match_is_good': '>=0.0')

println "Treshold\tPrecision\tRecall\tFScore"
maxFScore = 0
while (pr.scalars['vertex_count'].toDouble() > 0) {
  // Compute current precision/recall
  pr.aggregateVertexAttributeGlobally('aggregate-match_is_good': 'sum,count', prefix: '')
  matches = pr.scalars['match_is_good_count'].toDouble()
  good_matches = pr.scalars['match_is_good_sum'].toDouble()

  pr.aggregateVertexAttributeGlobally(
    'aggregate-linkedin_name similarity score': 'min',
    prefix: '')
  currentMin = pr.scalars['linkedin_name similarity score_min'].toDouble()
  recall = good_matches * 100.0 / matches
  precision = good_matches * 100.0 / testSetSize
  fScore = 2 / (1 / recall + 1 / precision)
  maxFScore = [fScore, maxFScore].max()
  System.out.printf(
    "%8.3f\t% 8.2f\t% 8.2f % 8.2f\n",
    currentMin,
    recall,
    precision,
    fScore)

  // Throw away lowest scoring matches.
  pr.filterByAttributes('filterva-linkedin_name similarity score': ">$currentMin")
}

println "Maximal fScore: $maxFScore"
