import lynx.kite
import sys
import copy
import time


# DRIVER

GLOBAL_TESTS = {}


def bdtest(input_names):
  'Decorator function for tests; see the usage later in the file'
  def inner(code):
    test_name = code.__name__
    GLOBAL_TESTS[test_name] = (input_names, code)
  return inner


def choose_one_that_can_run(tests, outputs):
  for t in tests:
    input_names = tests[t][0]
    if all([i in outputs for i in input_names]):
      return t
  return None


def run_tests(lk, tests, outputs, dry_run):
  while len(tests) > 0:
    test_name = choose_one_that_can_run(tests, outputs)
    if not test_name:
      print("I can't run these tests:", file=sys.stderr)
      for t in tests:
        missing = list(filter(lambda n: n not in outputs, tests[t][0]))
        print(f'test: {t} -  missing inputs: {missing}', file=sys.stderr)
      sys.exit(1)
    input_names, code = tests[test_name]
    inputs = [outputs[i] for i in input_names]
    if not dry_run:
      start = time.monotonic()
      result = code(lk, *inputs)
      result.compute()
      end = time.monotonic()
      print(f'Computing {test_name} took {end-start} seconds')
    else:
      result = 'dummy'
    outputs[test_name] = result
    del tests[test_name]


def main():
  lk = lynx.kite.LynxKite(address="http://localhost:2200")
  dummy_tests = copy.deepcopy(GLOBAL_TESTS)
  dummy_outputs = {}
  run_tests(lk, dummy_tests, dummy_outputs, dry_run=True)
  outputs = {}
  run_tests(lk, GLOBAL_TESTS, outputs, dry_run=False)


# TESTS


@bdtest([])
def vertices(lk):
  return lk.importParquetNow(filename='DATA$/exports/graph_10_vertices').useTableAsVertices()


@bdtest([])
def edges(lk):
  return lk.importParquetNow(filename='DATA$/exports/graph_10_edges').sql('select * from input')


@bdtest(['vertices', 'edges'])
def graph(lk, vertices, edges):
  return lk.useTableAsEdges(vertices, edges, attr='id', src='src_id', dst='dst_id')


@bdtest(['graph'])
def random_attributes(lk, r):
  seed = 12341
  dists = {'rnd_std_uniform': 'Standard Uniform', 'rnd_std_normal': 'Standard Normal'}
  for attr_name in dists:
    r = lk.addRandomVertexAttribute(r, name=attr_name,
                                    dist=dists[attr_name], seed=str(seed))
    seed += 1
    r = lk.addRandomEdgeAttribute(r, name=attr_name,
                                  dist=dists[attr_name], seed=str(seed))
    seed += 1
  return r


@bdtest(['graph'])
def degree(lk, r):
  return lk.computeDegree(r, direction='all edges', name='degree')


@bdtest(['graph'])
def centrality(lk, r):
  return lk.computeCentrality(r, algorithm='Harmonic', bits='4',
                              maxDiameter='5', name='centrality')


@bdtest(['graph'])
def approximate_clustering_coefficient(lk, r):
  return lk.approximateClusteringCoefficient(r, name='clustering_coefficient', bits='8')


@bdtest(['graph'])
def clustering_coefficient(lk, r):
  return lk.approximateClusteringCoefficient(r, name='clustering_coefficient')


@bdtest(['graph'])
def compute_embeddedness(lk, r):
  return lk.computeEmbeddedness(r, name='embeddedness')


@bdtest(['random_attributes'])
def segment_by_interval(lk, r):
  r = lk.addRandomVertexAttribute(r, name='rnd_std_normal2',
                                  dist='Standard Normal', seed='31415')
  r = lk.renameVertexAttributes(r, change_rnd_std_normal2='i_begin')
  r = lk.deriveVertexAttribute(r, output='i_end', expr='i_begin + Math.abs(rnd_std_normal)')
  r = lk.segmentByInterval(r, begin_attr='i_begin', end_attr='i_end',
                           interval_size='0.01', name='seg_interval', overlap='no')
  r = lk.segmentByInterval(r, begin_attr='i_begin', end_attr='i_end',
                           interval_size='0.01', name='seg_interval_overlap', overlap='yes')

  return r


@bdtest(['segment_by_interval'])
def weighted_aggregate_from_segmentation(lk, r):
  return lk.weightedAggregateFromSegmentation(r, apply_to_project='.seg_interval',
                                              weight='size', prefix='', aggregate_top='weighted_sum')


@bdtest(['segment_by_interval'])
def weighted_aggregate_to_segmentation(lk, r):
  return lk.weightedAggregateToSegmentation(r, apply_to_project='.seg_interval',
                                            weight='rnd_std_uniform', aggregate_rnd_std_normal='weighted_sum')


main()
