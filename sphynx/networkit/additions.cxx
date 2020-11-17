// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>
#include "additions.h"

using namespace NetworKit;

void graphToEdgeList(Graph& g, SphynxId* src, SphynxId* dst) {
  int i = 0;
  g.forEdges([&](const node u, const node v) {
      src[i] = u;
      dst[i] = v;
      i += 1;
  });
}
