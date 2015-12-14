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
union.unionWithAnotherProject(
  'id-attr': 'new_id',
  other: fb.rootCheckpointWithTitle('facebook for FP'))

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

// Mark already known vertices - these will be excluded from evals.
union.derivedVertexAttribute(
  expr: 'id_count - 1.0',
  output: 'known',
  type: 'double')


// Fingerprint.
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

union.renameVertexAttribute(
  from: 'linkedin_name similarity score',
  to: 'score')

testSetSize = union.scalars['test_set_size'].toDouble()
println "Test set size: $testSetSize"


// Import linkedin as segmentation, connect by attribute, fingerprint.
segmentationFP = fb.saveAs('linkedin facebook segmentation FP')

// Import as segmentation, connect based on merge key.
segmentationFP.importProjectAsSegmentation(
  them: linkedin.rootCheckpointWithTitle('linkedin for FP'))
segmentationFP.segmentations['linkedin for FP'].defineSegmentationLinksFromMatchingAttributes(
  'base-id-attr': 'merge_key_name',
  'seg-id-attr': 'merge_key_name')

// Mark already known vertices - these will be excluded from evals.
segmentationFP.segmentations['linkedin for FP'].aggregateFromSegmentation(
  'aggregate-id': 'count',
  prefix: 'linkedin_original')
segmentationFP.fillWithConstantDefaultValue(attr: 'linkedin_original_id_count', def: '0')
segmentationFP.renameVertexAttribute(
  from: 'linkedin_original_id_count',
  to: 'known')

// Go fingerprinting.
segmentationFP.segmentations['linkedin for FP'].fingerprintingBetweenProjectAndSegmentation(
  mo: '1',
  ms: '0.0',
  extra: '"weightingMode": "InverseInDegreeBasedHybrid", "multiNeighborsPreference": 5.0')

// Pull over some attributes.
segmentationFP.segmentations['linkedin for FP'].aggregateFromSegmentation(
  'aggregate-linkedin_name': 'most_common',
  'aggregate-test_matched_fb_name': 'most_common',
  prefix: 'linkedin for FP')
segmentationFP.renameVertexAttribute(
  from: 'linkedin for FP_linkedin_name_most_common',
  to: 'linkedin_name')
segmentationFP.renameVertexAttribute(
  from: 'linkedin for FP_test_matched_fb_name_most_common',
  to: 'test_matched_fb_name')

// Just of eyeballing convenience.
segmentationFP.derivedVertexAttribute(
  expr: 'linkedin_name + ":" + fb_name',
  output: 'names',
  type: 'string')

// Evaluate.
segmentationFP.renameVertexAttribute(
  from: 'fingerprinting_similarity_score',
  to: 'score')

segmentationFP.derivedVertexAttribute(
  expr: '(test_matched_fb_name === fb_name) ? 1.0 : 0.0',
  output: 'match_is_good',
  type: 'double')

segmentationFP.aggregateVertexAttributeGlobally(
  'aggregate-match_is_good': 'sum,count',
  'aggregate-test_matched_fb_name': 'count',
  prefix: '')

segmentationFP.renameScalar(from: 'match_is_good_count', to: 'matched_count')
segmentationFP.renameScalar(from: 'match_is_good_sum', to: 'correct_match_count')

// ======= Drawing a primitive P/R curve ============

def drawPR(pr) {

  pr.filterByAttributes('filterva-known': '=0.0')

  // It only makes sense to draw the PR curve at boundaries just before throwing away a good match.
  pr.derivedVertexAttribute(
    output: 'important_score',
    expr: '(match_is_good === 1.0) ? score : undefined',
    type: 'double')

  println "Threshold\tPrecision\tRecall\tFScore"
  maxFScore = 0

  pr.aggregateVertexAttributeGlobally('aggregate-important_score': 'min,count', prefix: '')
  while (pr.scalars['important_score_count'].toDouble() > 0) {
    nextMin = pr.scalars['important_score_min'].toDouble()
    // Filter just below the next important score.
    // Make sure we avoid scientific notation (which cannot be parsed on the other side)
    justBelow = String.format("%.18f", nextMin-0.000000001).toString()
    pr.filterByAttributes('filterva-score': ">$justBelow")

    // Compute current precision/recall
    pr.aggregateVertexAttributeGlobally(
      'aggregate-match_is_good': 'sum',
      'aggregate-score': 'count',
      prefix: '')
    matches = pr.scalars['score_count'].toDouble()
    good_matches = pr.scalars['match_is_good_sum'].toDouble()

    precision = good_matches * 100.0 / matches
    recall = good_matches * 100.0 / testSetSize
    fScore = 2 / (1 / recall + 1 / precision)
    maxFScore = [fScore, maxFScore].max()
    System.out.printf(
      "%8.3f\t% 8.2f\t% 8.2f % 8.2f\n",
      nextMin,
      precision,
      recall,
      fScore)

    // Throw away lowest scoring matches.
    // Make sure we avoid scientific notation (which cannot be parsed on the other side)
    justAbove = String.format("%.18f", nextMin+0.000000001).toString()
    pr.filterByAttributes('filterva-score': ">$justAbove")
    pr.aggregateVertexAttributeGlobally('aggregate-important_score': 'min,count', prefix: '')
  }
  println "Maximal fScore: $maxFScore"
}

prSeg = segmentationFP.saveAs('linkedin facebook segmentation PR curve')
drawPR(prSeg)

prAttr = union.saveAs('linkedin facebook PR curve')
drawPR(prAttr)
