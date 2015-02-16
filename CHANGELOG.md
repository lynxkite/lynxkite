<!---
Please add changes at the top. When releasing a version add a new header for that release.
-->

# Changes

### master

 - Implements colors and labels for sampled mode edges.
 - Discovered an issue that prevents compatibility from 1.0.0 and 1.0.1. Fortunately they were
   never marked as `stable`, so we are not making a heroic effort to maintain compatibility.
 - Visualize attributes of the type `(Double, Double)` as vertex positions.
   (No way to create such attributes yet, except for using the Example Graph.)

### 1.0.1

 - New setting in `.kiterc`: `YARN_NUM_EXECUTORS` can be used to specify the number of
   workers.
 - Added (better) workaround for https://issues.apache.org/jira/browse/SPARK-5102.
 - Select all vertices (up to 10,000) as center by setting `*` (star) as the center.
 - 3D graph visualization in concrete vertices view.

### 1.0.0

 - Adds label size as a new visualization option.
 - UI for selecting vertex attribute visualizations has changed as a preparation for adding more
   visualization options.
 - Export `Vector` type attributes as a semicolon-separated list.
 - Vertex ID is no longer displayed in the visualization by default.
   It is still accessible by setting the ID attribute as the label.
 - User management page at `/#/users`.
 - Added a check for Apache Spark version at startup. Must be 1.2.0.
 - Changed project data format to JSON. Projects from earlier versions are lost.
   Compatibility with future versions, however, is guaranteed.
 - Added error reporting option for the project list.
 - Cleared up confusion between edge graph/line graph/dual graph.
 - Started changelog.
