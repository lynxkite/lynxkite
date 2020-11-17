// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>

typedef uint32_t SphynxId;
void graphToEdgeList(NetworKit::Graph& g, SphynxId* src, SphynxId* dst);
