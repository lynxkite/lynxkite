#!/usr/bin/env python3
#
# This script runs big data tests. Example usage:
#
#   ./big_data_tests.py --test compute_embeddedness,centrality --vertex_file  'DATA$/exports/graph_10_vertices' --edge_file 'DATA$/exports/graph_10_edges'
#
# This runs two tests: compute_embeddedness and centrality, plus all other tests that these two depend on.
# The two input files specify the vertices and the edges. These must be in Parquet format.
#
# This command:
#
#   ./big_data_tests.py --vertex_file  'DATA$/exports/graph_10_vertices' --edge_file 'DATA$/exports/graph_10_edges'
#
# will run all the tests.
#
# You can add more tests at the end of this file; use the @bdtest directive.
#
#

import lynx.kite
import sys
import copy
import time
import argparse
from functools import reduce

# DRIVER

PARSER = argparse.ArgumentParser()
PARSER.add_argument('--tests', type=str, default='all',
                    help='Comma separated list of the tests to run. The default "all" means run all.')
PARSER.add_argument('--vertex_file', type=str, required=True,
                    help='LynxKite-type (prefixed) path to the Parquet file specifying the vertices, e.g., DATA$/exports/testgraph_vertices')

PARSER.add_argument('--edge_file', type=str, required=True,
                    help='LynxKite-type (prefixed) path to the Parquet file specifying the edges, e.g., DATA$/exports/testgraph_edges')

PARSER.add_argument('--src', type=str, default='src_id',
                    help='The name of the src column in the edges file')

PARSER.add_argument('--dst', type=str, default='dst_id',
                    help='The name of the dst column in the edges file')


ARGS = PARSER.parse_args()

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


def all_input_names(tests):
  w = [v[0] for v in tests.values()]
  return reduce(lambda x, y: x + y, w)


def get_a_test_that_can_be_removed(tests, tests_that_must_run):
  input_names = all_input_names(tests)
  for t in tests:
    if not t in input_names and not t in tests_that_must_run:
      return t
  return None


def get_only_necessary_tests(all_tests, tests_that_must_run):
  tests = copy.deepcopy(all_tests)
  while True:
    w = get_a_test_that_can_be_removed(tests, tests_that_must_run)
    if not w:
      return tests
    del tests[w]


def main():
  lk = lynx.kite.LynxKite()
  dummy_tests = copy.deepcopy(GLOBAL_TESTS)
  dummy_outputs = {}
  run_tests(lk, dummy_tests, dummy_outputs, dry_run=True)
  outputs = {}
  if ARGS.tests == 'all':
    run_tests(lk, GLOBAL_TESTS, outputs, dry_run=False)
  else:
    tests_to_run = ARGS.tests.split(',')
    for t in tests_to_run:
      if t not in GLOBAL_TESTS:
        print(f'Unknown test name: {t}', file=sys.stderr)
        print(f'Possible values: {list(GLOBAL_TESTS.keys())}')
        sys.exit(1)
    necessary_tests = get_only_necessary_tests(GLOBAL_TESTS, tests_to_run)
    run_tests(lk, necessary_tests, outputs, dry_run=False)


# TESTS


@bdtest([])
def vertices(lk):
  return lk.importParquetNow(filename=ARGS.vertex_file).useTableAsVertices()


@bdtest([])
def edges(lk):
  return lk.importParquetNow(filename=ARGS.edge_file).sql('select * from input')


@bdtest(['vertices', 'edges'])
def graph(lk, vertices, edges):
  return lk.useTableAsEdges(vertices, edges, attr='id', src=ARGS.src, dst=ARGS.dst)


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

    r = lk.addRandomVertexAttribute(r, name='rnd_std_normal2',
                                    dist='Standard Normal', seed=str(seed))
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


@bdtest(['random_attributes'])
def segmentations(lk, r):
  r = lk.segmentByDoubleAttribute(r, attr='rnd_std_normal',
                                  interval_size='0.01', name='seg', overlap='no')
  r = lk.segmentByDoubleAttribute(r, attr='rnd_std_normal',
                                  interval_size='0.01', name='seg_overlap', overlap='yes')
  r = lk.deriveVertexAttribute(r, output='x', expr='"%.7f".format(rnd_std_uniform)')
  r = lk.segmentByStringAttribute(r, attr='x', name='seg_string')
  return r


@bdtest(['segmentations'])
def combine_segmentations(lk, r):
  return lk.combineSegmentations(r, name='seg_combined', segmentations='seg,seg_overlap,seg_string')


@bdtest(['graph'])
def find_connected_components(lk, r):
  return lk.findConnectedComponents(r, name='connected_components', directions='ignore directions')


main()
