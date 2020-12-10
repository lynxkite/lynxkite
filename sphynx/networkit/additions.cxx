// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>
#include <networkit/distance/Diameter.hpp>
#include "additions.h"

using namespace NetworKit;

void graphToEdgeList(Graph* g, SphynxId* src, SphynxId* dst) {
  int i = 0;
  g->forEdges([&](const node u, const node v) {
      src[i] = u;
      dst[i] = v;
      i += 1;
  });
}

// Simpler than wrapping pairs.
double diameterLower(Diameter* d) {
  return d->getDiameter().first;
}
double diameterUpper(Diameter* d) {
  return d->getDiameter().second;
}
