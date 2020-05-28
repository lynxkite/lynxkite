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

  parser.add_argument('--src', type=str, default='src',
                      help='The name of the src column in the edges file')

  parser.add_argument('--dst', type=str, default='dst',
                      help='The name of the dst column in the edges file')

  parser.add_argument('--vertex_set_size', type=int, default=100000,
                      help='The vertex set size for tests that want to start with "Create Vertrices"')
  return parser.parse_args()


ARGS = get_args()
TESTS = {}
LK = lynx.kite.LynxKite()
COMPUTE_RESULT = namedtuple('COMPUTE_RESULT', ['lk_state', 'time_taken', 'test_name'])


def bdtest():
  '''Decorator function for tests; see the usage later in the file'''
  def inner(op):
    input_names = list(signature(op).parameters.keys())
    assert(all([i in TESTS for i in input_names]))
    TESTS[op.__name__] = (input_names, op)
  return inner


@lru_cache(maxsize=None)
def compute(test):
  input_names, op = TESTS[test]
  inputs = [compute(inp_name).lk_state for inp_name in input_names]
  print(f'Running {test}', file=sys.stderr)
  start = time.monotonic()
  lk_state = op(*inputs)
  lk_state.compute()
  time_taken = time.monotonic() - start
  return COMPUTE_RESULT(lk_state=lk_state, time_taken=time_taken, test_name=test)


def main():
  tests_to_run = ARGS.tests.split(',') if ARGS.tests != 'all' else list(TESTS.keys())
  unknown = [t for t in tests_to_run if t not in TESTS.keys()]
  assert len(unknown) == 0, f'Unknown test(s): {unknown}\nAvaliable: {TESTS.keys()}'
  for result in [compute(t) for t in tests_to_run]:
    t = "{:.2f}".format(result.time_taken)
    print(f'Computing {result.test_name} took {result.time_taken:.2f} seconds')


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
  return LK.useTableAsEdges(vertices, edges, attr='vertex_id', src=ARGS.src, dst=ARGS.dst)


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
  graph = LK.addRankAttribute(graph, rankattr='ordinal', keyattr='rnd_std_normal')
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
  return LK.weightedAggregateFromSegmentation(segment_by_interval, apply_to_graph='.seg_interval',
                                              weight='size', prefix='', aggregate_top='weighted_sum')


@bdtest()
def weighted_aggregate_to_segmentation(segment_by_interval):
  return LK.weightedAggregateToSegmentation(segment_by_interval, apply_to_graph='.seg_interval',
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


@bdtest()
def filter_high_degree_vertices(degree):
  return LK.filterByAttributes(degree, filterva_degree='<5000')


@bdtest()
def filter_high_degree_vertices_1000(degree):
  return LK.filterByAttributes(degree, filterva_degree='<1000')


@bdtest()
def find_maximal_cliques(filter_high_degree_vertices):
  return LK.findMaximalCliques(filter_high_degree_vertices, bothdir='true',
                               name='maximal_cliques', min='3')


@bdtest()
def create_edges_from_cooccurrence(find_maximal_cliques):
  edgeless = LK.discardEdges(find_maximal_cliques)
  return LK.createEdgesFromCooccurrence(edgeless, apply_to_graph='.maximal_cliques')


@bdtest()
def create_edges_from_cooccurrence(find_maximal_cliques):
  edgeless = LK.discardEdges(find_maximal_cliques)
  return LK.createEdgesFromCooccurrence(edgeless, apply_to_graph='.maximal_cliques')


@bdtest()
def self_segmentation(random_attributes):
  edgeless = LK.discardEdges(random_attributes)
  s = LK.useBaseProjectAsSegmentation(edgeless, name='segmentation')
  return s


@bdtest()
def define_segmentation_link_from_matching_attributes(self_segmentation):
  s = self_segmentation
  s = LK.deriveVertexAttribute(s, output='src', expr='Math.floor(ordinal/3.0).toString')
  s = LK.deriveVertexAttribute(s, apply_to_graph='.segmentation',
                               output='dst', expr='Math.floor(ordinal/5.0).toString')
  s = LK.defineSegmentationLinksFromMatchingAttributes(s, apply_to_graph='.segmentation',
                                                       base_id_attr='src', seg_id_attr='dst')

  return s.sql('select * from `segmentation.belongs_to`')


@bdtest()
def find_triangles(filter_high_degree_vertices_1000):
  return LK.findTriangles(filter_high_degree_vertices_1000, bothdir='false', name='triangles')


@bdtest()
def find_infocom_communities(find_maximal_cliques):
  return LK.findInfocomCommunities(find_maximal_cliques,
                                   cliques_name='maximal_cliques',
                                   communities_name='communities',
                                   bothdir='false',
                                   min_cliques='3',
                                   adjacency_threshold='0.6')


@bdtest()
def merge_parallel_edges_by_attribute(random_attributes):
  g = LK.deriveEdgeAttribute(
      random_attributes,
      output='label',
      expr='Math.floor(rnd_std_uniform*2)')
  return LK.mergeParallelEdgesByAttribute(g, key='label', aggregate_label='average')


@bdtest()
def random_attributes_with_constants(random_attributes):
  g = LK.addConstantVertexAttribute(random_attributes, name='one', value='1.0', type='Double')
  g = LK.aggregateVertexAttributeGlobally(g, prefix='', aggregate_one='sum')
  return LK.renameScalar(g, before='one_sum', after='number_of_vertices')


@bdtest()
def merge_vertices_by_attribute(random_attributes_with_constants):
  g = LK.deriveVertexAttribute(random_attributes_with_constants, output='label',
                               expr='Math.floor(rnd_std_uniform*number_of_vertices*0.01)')
  return LK.mergeVerticesByAttribute(g, key='label', aggregate_rnd_std_normal='average')


@bdtest()
def even_distribution():
  a = ARGS.vertex_set_size
  b = a // 10
  g = LK.createVertices(size=a)
  g = LK.addRandomVertexAttribute(
      g,
      name='rnd_std_uniform',
      dist='Standard Uniform',
      seed='4242567')
  g = LK.addRandomVertexAttribute(g, name='rnd_std_normal', dist='Standard Normal', seed='4242568')
  g = LK.deriveVertexAttribute(g, output='label', expr=f'Math.floor(rnd_std_uniform*{b})')
  g = LK.deriveVertexAttribute(g, output='label2', expr=f'Math.floor(rnd_std_normal)')
  return g


@bdtest()
def merge_vertices_by_attribute_even(even_distribution):
  return LK.mergeVerticesByAttribute(
      even_distribution, key='label', aggregate_rnd_std_uniform='average')


@bdtest()
def merge_vertices_by_attribute_longtail(even_distribution):
  return LK.mergeVerticesByAttribute(even_distribution, key='label2',
                                     aggregate_rnd_std_uniform='average,most_common')


@bdtest()
def find_modular_clustering(filter_high_degree_vertices):
  return LK.findModularClustering(filter_high_degree_vertices, name='modular_clusters', weights='!no weight',
                                  max_iterations='30', min_increment_per_iteration='0.001')


@bdtest()
def compute_pagerank(random_attributes):
  g = random_attributes
  g = LK.computePageRank(
      g,
      name='page_rank_no_weights',
      weights='!no weight',
      iterations='5',
      damping='0.85')
  g = LK.computePageRank(
      g,
      name='page_rank_weights',
      weights='rnd_std_uniform',
      iterations='5',
      damping='0.85')
  return g


@bdtest()
def create_snowball_sample(graph):
  return LK.createSnowballSample(graph, attrName='distance_from_start_point',
                                 ratio='0.0001', radius='1', seed='123454321')


@bdtest()
def replace_edges_with_triadic_closure(filter_high_degree_vertices_1000):
  g = LK.filterByAttributes(filter_high_degree_vertices_1000, filterva_degree='<100')
  g = LK.addRandomEdgeAttribute(g, name='attr', dist='Standard Uniform', seed='1234321')
  return LK.replaceEdgesWithTriadicClosure(g)


@bdtest()
def project_union(graph):
  return LK.projectUnion(graph, graph)


@bdtest()
def weighted_aggregate_edge_attribute_to_vertices(random_attributes):
  return LK.weightedAggregateEdgeAttributeToVertices(random_attributes, prefix='',
                                                     weight='rnd_std_uniform',
                                                     direction='all edges',
                                                     aggregate_rnd_std_normal='weighted_average')


@bdtest()
def weighted_aggregate_on_neighbors(random_attributes):
  return LK.weightedAggregateOnNeighbors(random_attributes, prefix='',
                                         weight='rnd_std_uniform',
                                         direction='all edges',
                                         aggregate_rnd_std_normal='weighted_average')


@bdtest()
def scala(random_attributes):
  return LK.deriveEdgeAttribute(random_attributes, output='x',
                                expr='rnd_std_uniform*rnd_std_uniform')


main()
