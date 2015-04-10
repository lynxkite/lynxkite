<!---
Please add changes at the top. When releasing a version add a new header for that release.
-->

# Changes

### master

### 1.3.0

 - New vertex operation "Add rank attribute" make it possible for the user to sort an
   attribute (of double type). This can be used to identify vertices which have the
   highest of lowest values with respect to some attributes.
 - Improved error checking for JavaScript expressions. (In "derive" operations.)
 - Fixed project history performance issue.
 - More than one type of aggregation can be selected for each attribute in one operation.
 - New _"Combine segmentations"_ operation makes it possible to create a segmentation
   by cities of residence _and_ age, for example.
 - New _"Segment by double attribute"_ and _"Segment by string attribute"_ operations
   make it possible to create a segmentation by cities of residence or age, for example.
 - New _"Create edges from co-occurrence"_ operation creates edges in the parent project
   for each pair of vertices that co-occur in a segment. If they co-occur multiple times,
   multiple parallel edges are created.

### 1.2.1

 - Backward compatibility for undo in pre-1.2.0 projects.

### 1.2.0

 - Graph visualizations are now cut to only show on the corresponding side. The previous
   behavior was especially disturbing when combined with map view.
 - The user registry file now has to be specified in `.kiterc` as `KITE_USERS_FILE`.
   Earlier versions used the user registry from `kite_XXX/conf/users.txt`. If you run
   LynxKite with authentication, please copy this file to a preferable location and add
   `KITE_USERS_FILE` to your `.kiterc` when upgrading.
 - Improvements in the configuration for number of cores per executor:
   - One common .kiterc option (`NUM_CORES_PER_EXECUTOR`) regardless of deployment mode
   - As a consequence, `YARN_CORES_PER_EXECUTOR` option has been removed. Please update
     .kiterc on YARN deployments by renaming `YARN_CORES_PER_EXECUTOR` to `NUM_CORES_PER_EXECUTOR`.
   - Use correcty the value of the above option internally to optimize calculations knowing
     the amount of cores available
   - In case of EC2, correctly set up this new kiterc option based on the cluster configuration
 - Fixed a bug that caused "hard" filtering to segmentations to fail.
 - Spaces in ACL lists are now ignored.
 - Error reports now go to `support@lynxanalytics.freshdesk.com` instead of
   `pizza-support@lynxanalytics.com`.
 - Previously hidden utility operations (like _"Delete vertex attribute"_) are now revealed
   on the interface. This is so that they can be accessed when editing the history.
 - Import and simple operations should now work even if there is insufficient memory to
   hold the entire graph.
 - New icons: `triangle`, `pentagon`, `star`, `sim`, `radio`.
 - File sizes are logged for import and upload. (This will be useful for troubleshooting.)
 - Operation search button. (This is the same as pressing `/`.)
 - Delimiters in load CSV operations now support standard java escapes. (Most importantly,
   you can import from a tab delimited file using `\t` as the value of the delimiter
   parameter.)
 - Project history (the list of operations that have been applied) can now be viewed and
   also edited. This is an experimental feature. Operations from earlier versions cannot be
   viewed or edited.
 - Fixed an issue with undo. The fix introduces an undo incompatibility. Operations
   from earlier versions cannot be undone after the upgrade.
 - Added a "Filter by attributes" operation. This is identical to filtering the usual way.
 - Regex filters can now be applied to string attributes. The syntax is `regex(R)` for
   example for finding strings that contain `R`. Or `regex(^Ab)` for strings that start
   with `Ab`. Or `regex((.)\1)` for strings with double letters. Etc.
 - Non-neighboring vertices are partially faded out when hovering over a vertex.
 - Fixed a leak in visualization that would gradually degrade performance in the sampled
   view.
 - In addition to scrolling, double-clicking and right double-clicking can be used now
   to zoom in and out. With Shift they adjust the thickness.
 - Upgraded to Apache Spark 1.3.0. Users need to download this and update `SPARK_HOME`
   in their `.kiterc`.
 - Fixed logout behavior on the splash page (`/`).

### 1.1.3

 - Fixed a bug with user creation.

### 1.1.2

 - Administrator users can now be created. Only administrators can create new users, and
   they have full access to all projects. Old user configuration files are incompatible with
   this change. If this is a problem, we can manually convert old user configuration files.
 - Add logout link at the bottom when authentication is enabled.

### 1.1.1

 - _"Connect vertices on attribute"_ now takes two attributes as parameters and will
   connect A to B if the "source" attribute of A matches the "destination" attribute
   of B. Set the same attribute for both parameters to get the previous behavior.
 - Fixes a bug in edge visualizations (manifested as
   "java.util.NoSuchElementException: key not found: ...")

### 1.1.0

 - Authentication related UI is hidden when authentication is disabled.
 - Position (latitude + longitude) attributes can be visualized on a map.
   (The map is generated by Google Maps, but none of the graph data is sent to Google.)
 - New edge operation "Add reversed edges" which creates a symmetric graph.
 - When a project is hard filtered results are now saved to disk. Previously they behaved
   identically to soft filters that - correctly - only compute their results on the fly.
 - Edge attributes can be visualized as colors and labels in sampled mode.
 - Discovered an issue that prevents compatibility from 1.0.0 and 1.0.1. Fortunately they were
   never marked as `stable`, so we are not making a heroic effort to maintain compatibility.
 - Visualize attributes of the type `(Double, Double)` as vertex positions.
   Such attributes can be created from two `Double` attributes with the new
   _"Vertex attributes to position"_ operation.

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
