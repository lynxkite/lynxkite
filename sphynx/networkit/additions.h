// Additional code for the C++ side to simplify the interface.
#include <vector>
#include <networkit/Globals.hpp>
#include <networkit/graph/Graph.hpp>
#include <networkit/distance/Diameter.hpp>
#include <networkit/edgescores/EdgeScore.hpp>

typedef uint32_t SphynxId;
void graphToEdgeList(NetworKit::Graph* g, SphynxId* src, SphynxId* dst);
double diameterLower(NetworKit::Diameter* d);
double diameterUpper(NetworKit::Diameter* d);

// We've got to count triangles and then take 1-JaccardDistance.
// This is basically an EdgeScore<double>, but it's simpler for SWIG this way.
class JaccardSimilarity {
  public:
    JaccardSimilarity(const NetworKit::Graph &G);
    virtual const std::vector<double> &scores() const;
    void run();
  protected:
    const NetworKit::Graph *G;
    std::vector<double> scoreData;
};
