<!---
Please add changes at the top. When releasing a version add a new header for that release.
-->

# Changes

### master

 - Select all vertices (up to 10,000) as center by setting "*" as the center.
 - 3D graph visualization in concrete vertices view.
 - User management page at `/#/users`.
 - Added a check for Apache Spark version at startup. Must be 1.2.0.
 - Changed project data format to JSON. Projects from earlier versions are lost.
   Compatibility with future versions, however, is guaranteed.
 - Added error reporting option for the project list.
 - Cleared up confusion between edge graph/line graph/dual graph.
 - Started changelog.
