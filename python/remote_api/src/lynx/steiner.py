'''Glue code for converting csv files to the dapcstp format and converting the reuslts
back to a LynxKite graph'''
import pandas as pd

import grpc
from lynx.proto import dapcstp_pb2 as pb
from lynx.proto import dapcstp_pb2_grpc

import lynx.kite
import re


@lynx.kite.external
def _steiner(address, vertices, edges):
  v = vertices.pandas().sort_values(by='ranking')
  e = edges.pandas()
  with grpc.insecure_channel(address) as channel:
    stub = dapcstp_pb2_grpc.FiberOptimizerStub(channel)
    assert list(v.ranking) == [float(i) for i in range(v.shape[0])]
    g = pb.Graph()
    for _, row in v.iterrows():
      n = g.node.add()
      n.cost = row['cost']
      assert n.cost >= 0
      n.prize = row['prize']
      assert n.prize >= 0
    for _, row in e.iterrows():
      a = g.arc.add()
      a.src = int(row['src_ranking'])
      a.dst = int(row['dst_ranking'])
      a.cost = row['edge_cost']
      assert a.cost >= 0

    solution = stub.Optimize(pb.OptimizeRequest(graph=g))
    res = [(str(e.src), str(e.dst)) for e in solution.graph.arc]
    return pd.DataFrame(res, columns=['src', 'dst'])


@lynx.kite.subworkspace
def _join_result(lk, problem, solution):
  rank_str = problem.deriveVertexAttribute(output='s_ranking', expr='ranking.toInt.toString')
  vertex_merged = lk.useTableAsVertexAttributes(solution, rank_str, id_attr='stringId', id_column='s_ranking', prefix='o')\
      .deriveEdgeAttribute(output='real_id', expr='src$stringId.toString + "_" + dst$stringId.toString')

  edge_id = problem.deriveEdgeAttribute(
      output='real_id',
      expr='src$ranking.toInt.toString + "_" + dst$ranking.toInt.toString').sql('select * from edges')

  fully_merged = lk.useTableAsEdgeAttributes(
      vertex_merged,
      edge_id,
      id_attr='real_id',
      id_column='edge_real_id',
      prefix='ee')

  profit = fully_merged.aggregateEdgeAttributeGlobally(aggregate_ee_edge_cost='sum')\
      .aggregateVertexAttributeGlobally(aggregate_o_cost='sum', aggregate_o_prize='sum')\
      .deriveScalar(output='profit', expr='o_prize_sum - o_cost_sum - ee_edge_cost_sum')\
      .renameEdgeAttributes(change_ee_src_prize='', change_src='', change_ee_edge_src='src', change_ee_src_cost='', change_ee_edge_dst='dst', change_ee_src_ranking='', change_ee_edge_cost='cost', change_dst='', change_ee_dst_id='', change_ee_dst_cost='', change_ee_dst_prize='prize', change_ee_edge_real_id='', change_ee_dst_ranking='', change_real_id='', change_ee_src_id='')\
      .renameVertexAttributes(change_o_prize='prize', change_o_cost='cost', change_o_s_ranking='', change_o_ranking='', change_stringId='', change_o_id='original_id')\
      .discardScalars(name='ee_edge_cost_sum,o_cost_sum,o_prize_sum')

  return profit


def get_grpc_address(lk):
  lk_address = lk.address()
  return re.search(r'https?://([a-zA-Z\.]*)', lk_address).groups()[0] + ":5656"


def optimize(fiber_graph):
  '''Calculates an approximate solution for the steiner tree problem.'''
  lk = fiber_graph.lk
  d_vertices = fiber_graph.sql('select * from vertices')
  d_edges = fiber_graph.sql('select * from edges')

  grpc_address = get_grpc_address(lk)
  d = _steiner(grpc_address, d_vertices, d_edges)
  d.trigger()

  solution = d.useTableAsGraph(src='src', dst='dst')
  result = _join_result(lk, fiber_graph, solution)
  return result


def _upload(lk, filename):
  with open(filename, 'rt') as f:
    return lk.importCSVNow(filename=lk.upload(f), infer="no")


def fiber_graph(lk, vertex_path, edge_path):
  '''Constructs a LynxKite graph in the appropriate format for `optimize`.
  The files are expected to be in the following formats:

  vertices.csv:
  id,cost,prize
  0,10,0
  1,0,5

  edge.csv:
  src,dst,cost
  0,1,1
  '''
  vertices = _upload(lk, vertex_path).useTableAsVertices()
  edges = _upload(lk, edge_path)
  problem = lk.useTableAsEdges(vertices, edges, attr='id', src='src', dst='dst')\
      .addRankAttribute(keyattr='id')\
      .convertEdgeAttributeToDouble(attr='cost')\
      .convertVertexAttributeToDouble(attr='cost,prize')
  return problem
