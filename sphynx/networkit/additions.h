// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>

struct EdgeList {
  NetworKit::count n;
  std::vector<unsigned long long> src, dst;
};

EdgeList graphToEdgeList(NetworKit::Graph& g);
