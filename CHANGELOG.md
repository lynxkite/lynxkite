<!---
Please add changes at the top. When releasing a version add a new header for that release.
-->

# Changes

### master

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
