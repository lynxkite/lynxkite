#!/usr/bin/env python3
'''
This script runs big data tests. Example usage:

   ./big_data_tests.py --test compute_embeddedness,centrality --vertex_file  'DATA$/exports/graph_10_vertices' --edge_file 'DATA$/exports/graph_10_edges'

This runs two tests: compute_embeddedness and centrality, plus all other tests that these two depend on.
The two input files specify the vertices and the edges. These must be in Parquet format.

This command:

  ./big_data_tests.py --vertex_file  'DATA$/exports/graph_10_vertices' --edge_file 'DATA$/exports/graph_10_edges'

will run all the tests.

You can add more tests at the end of this file; use the @bdtest directive.
'''

import lynx.kite
import sys
import time
import argparse
from inspect import signature
from functools import lru_cache
from collections import namedtuple


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--tests', type=str, default='all',
                      help='Comma separated list of the tests to run. The default "all" means run all.')
  parser.add_argument('--vertex_file', type=str, required=True,
                      help='LynxKite-type (prefixed) path to the Parquet file specifying the vertices, e.g., DATA$/exports/testgraph_vertices')

  parser.add_argument('--edge_file', type=str, required=True,
                      help='LynxKite-type (prefixed) path to the Parquet file specifying the edges, e.g., DATA$/exports/testgraph_edges')

  parser.add_argument('--src', type=str, default='src_id',
                      help='The name of the src column in the edges file')

  parser.add_argument('--dst', type=str, default='dst_id',
                      help='The name of the dst column in the edges file')
  return parser.parse_args()


ARGS = get_args()
TESTS = {}
LK = lynx.kite.LynxKite()
COMPUTE_RESULT = namedtuple('COMPUTE_RESULT', ['lk_state', 'time', 'test_name'])


def bdtest():
  '''Decorator function for tests; see the usage later in the file'''
  def inner(op):
    sig = str(signature(op)).replace(' ', '')
    input_names = [] if sig == '()' else sig[1:-1].split(',')
    assert(all([i in TESTS for i in input_names]))
    TESTS[op.__name__] = (input_names, op)
  return inner


@lru_cache(maxsize=None)
def compute(test):
  input_names, op = TESTS[test]
  inputs = [compute(inp_name).lk_state for inp_name in input_names]
  start = time.monotonic()
  state = op(*inputs)
  state.compute()
  time_taken = time.monotonic() - start
  return COMPUTE_RESULT(state, time_taken, test)


def main():
  tests_to_run = ARGS.tests.split(',') if ARGS.tests != 'all' else list(TESTS.keys())
  assert(all([t in TESTS for t in tests_to_run]))
  for result in [compute(t) for t in tests_to_run]:
    print(f'Computing {result.test_name} took {result.time} seconds')


# TESTS
# Make sure that each input parameter name is the same as the function name
# that generated that input.


@bdtest()
def vertices():
  return LK.importParquetNow(filename=ARGS.vertex_file).useTableAsVertices()


@bdtest()
def edges():
  return LK.importParquetNow(filename=ARGS.edge_file).sql('select * from input')


@bdtest()
def graph(vertices, edges):
  return LK.useTableAsEdges(vertices, edges, attr='id', src=ARGS.src, dst=ARGS.dst)


@bdtest()
def random_attributes(graph):
  seed = 12341
  dists = {'rnd_std_uniform': 'Standard Uniform', 'rnd_std_normal': 'Standard Normal'}
  for attr_name in dists:
    graph = LK.addRandomVertexAttribute(graph, name=attr_name,
                                        dist=dists[attr_name], seed=str(seed))
    seed += 1
    graph = LK.addRandomEdgeAttribute(graph, name=attr_name,
                                      dist=dists[attr_name], seed=str(seed))
    seed += 1

  graph = LK.addRandomVertexAttribute(graph, name='rnd_std_normal2',
                                      dist='Standard Normal', seed=str(seed))
  return graph


@bdtest()
def degree(graph):
  return LK.computeDegree(graph, direction='all edges', name='degree')


@bdtest()
def centrality(graph):
  return LK.computeCentrality(graph, algorithm='Harmonic', bits='4',
                              maxDiameter='5', name='centrality')


@bdtest()
def approximate_clustering_coefficient(graph):
  return LK.approximateClusteringCoefficient(graph, name='clustering_coefficient', bits='8')


@bdtest()
def clustering_coefficient(graph):
  return LK.approximateClusteringCoefficient(graph, name='clustering_coefficient')


@bdtest()
def compute_embeddedness(graph):
  return LK.computeEmbeddedness(graph, name='embeddedness')


@bdtest()
def segment_by_interval(random_attributes):
  r = random_attributes
  r = LK.renameVertexAttributes(r, change_rnd_std_normal2='i_begin')
  r = LK.deriveVertexAttribute(r, output='i_end', expr='i_begin + Math.abs(rnd_std_normal)')
  r = LK.segmentByInterval(r, begin_attr='i_begin', end_attr='i_end',
                           interval_size='0.01', name='seg_interval', overlap='no')
  r = LK.segmentByInterval(r, begin_attr='i_begin', end_attr='i_end',
                           interval_size='0.01', name='seg_interval_overlap', overlap='yes')

  return r


@bdtest()
def weighted_aggregate_from_segmentation(segment_by_interval):
  return LK.weightedAggregateFromSegmentation(segment_by_interval, apply_to_project='.seg_interval',
                                              weight='size', prefix='', aggregate_top='weighted_sum')


@bdtest()
def weighted_aggregate_to_segmentation(segment_by_interval):
  return LK.weightedAggregateToSegmentation(segment_by_interval, apply_to_project='.seg_interval',
                                            weight='rnd_std_uniform', aggregate_rnd_std_normal='weighted_sum')


@bdtest()
def segmentations(random_attributes):
  r = random_attributes
  r = LK.segmentByDoubleAttribute(r, attr='rnd_std_normal',
                                  interval_size='0.01', name='seg', overlap='no')
  r = LK.segmentByDoubleAttribute(r, attr='rnd_std_normal',
                                  interval_size='0.01', name='seg_overlap', overlap='yes')
  r = LK.deriveVertexAttribute(r, output='x', expr='"%.7f".format(rnd_std_uniform)')
  r = LK.segmentByStringAttribute(r, attr='x', name='seg_string')
  return r


@bdtest()
def combine_segmentations(segmentations):
  return LK.combineSegmentations(segmentations, name='seg_combined',
                                 segmentations='seg,seg_overlap,seg_string')


@bdtest()
def find_connected_components(graph):
  return LK.findConnectedComponents(
      graph, name='connected_components', directions='ignore directions')


main()
