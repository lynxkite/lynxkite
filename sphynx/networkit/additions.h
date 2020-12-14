// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>
#include <networkit/distance/Diameter.hpp>

typedef uint32_t SphynxId;
void graphToEdgeList(NetworKit::Graph* g, SphynxId* src, SphynxId* dst);
double diameterLower(NetworKit::Diameter* d);
double diameterUpper(NetworKit::Diameter* d);
