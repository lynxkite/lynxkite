// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>
#include "additions.h"

using namespace NetworKit;

EdgeList graphToEdgeList(Graph& g) {
  EdgeList el;
  el.n = g.numberOfNodes();
  el.src.reserve(g.numberOfEdges());
  el.dst.reserve(g.numberOfEdges());
  g.forEdges([&](const node u, const node v) {
      el.src.push_back(u);
      el.dst.push_back(v);
  });
  return el;
}
