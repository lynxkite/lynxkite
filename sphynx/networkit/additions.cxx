// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>
#include <networkit/distance/Diameter.hpp>
#include <networkit/distance/JaccardDistance.hpp>
#include <networkit/edgescores/EdgeScore.hpp>
#include <networkit/edgescores/TriangleEdgeScore.hpp>
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

#include <iostream>

JaccardSimilarity::JaccardSimilarity(const Graph &G) : G(&G), scoreData() {}
void JaccardSimilarity::run() {
  auto ts = new TriangleEdgeScore(*G);
  ts->run();
  std::cout << "ts: " << ts->scores()[0] << ", " << ts->scores()[1] << ", " << ts->scores()[2] << ", " << ts->scores()[3] << "\n";
  std::vector<count> triangles = ts->scores();
  std::cout << "ts: " << triangles[0] << ", " << triangles[1] << ", " << triangles[2] << ", " << triangles[3] << "\n";
  auto jd = new JaccardDistance(*G, triangles);
  jd->preprocess();
  std::vector<double> distance = jd->getEdgeScores();
  std::vector<double> similarity(G->upperEdgeIdBound(), 0);
  G->parallelForEdges([&](node u, node v, edgeid eid) {
    similarity[eid] = 1.0 - distance[eid];
    std::cout << eid << ": d=" << distance[eid] << ", s=" << similarity[eid] << ", t=" << triangles[eid] << "\n";
  });
  scoreData = std::move(similarity);
}
const std::vector<double> &JaccardSimilarity::scores() const {
  return scoreData;
}
