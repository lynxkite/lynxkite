<!---
Please add changes to "master". When releasing a version add a new header for that release.
-->

# Changes

### master

 - Dispersion computation made significantly faster.
 - Projects can be organized into folders now.
 - Logarithmic histograms support non-positive values.
 - New parameter for CSV import operations: `Tolerate ill-formed lines`. It controls
   whether or not non-conforming lines in the csv file should be skipped silently
   or cause an error immediately.
 - If the user requests a visualization that's too large (and would probably kill the browser)
   we return an error instead of trying to display the large visualization.
 - Users can now import additional attributes from CSV/SQL for edges as well (until now, it was
   only possible for vertices).
 - The internal identification of Kite meta graph entities depended on Java's default character
   encodig which in turn depended on the terminal setting of the user starting Kite. This resulted
   in Kite failure if a user with different settings restarted the server. Now this is fixed by
   alwasy using UTF-8 encoding. But this will cause problems if your kite instane uses something
   different (UTF-8 seems to be the typical, though). If Kite fails with cannot load project list
   and you see and error in the logs starting with "Output mismatch on ...", then try to force a
   migration: `KITE_FORCED_MIGRATION=true  ./run-kite-....sh restart`. Do this only once, not for
   all restarts in the future!

### 1.5.1

 - Workflow and batch API scripts now use Groovy instead of JSON. This makes them easier to read
   and the batch API gets more flexible too.
 - Users can now configure better the stopping condition for modular clustering.
 - Improved format for the graph storage. Note that this breaks compatibility of the data directory with 1.5.0.
   Compatibility is retained with all version before 1.5. One way to fix a data directory created/touched by Kite 
   1.5.0 is to delete the directory `$KITE_DATA_DIR/partitioned`.

### 1.5.0

 - Kite configuration setting `YARN_NUM_EXECUTORS` is replaced by the more general `NUM_EXECUTORS`
   which applies to the standalone spark cluster setup as well.
 - Reorganized operation categories. We hope you find them more logical.
 - The _Batch processing API_ is now ready for use. It allows you to run a sequence of operations
   from the command line. For more details see the _Batch processing API_ section in the manual.
 - Richer progress indicator.
 - LynxKite on EC2 will now always use ephemeral HDFS to store data. This data is lost if you `stop`
   or `destroy` the cluster. Use the new `s3copy` command if you want to save the data to S3.
   (The data does not have to be restored from S3 to HDFS. It will be read directly from S3.)
   This also means significant performance improvements for EC2 clusters.
 - User passwords can now be changed.
 - New operation _Metagraph_ is useful for debugging and perhaps also for demos.
 - Added an experimental tool for cleaning up old data. It is accessible as `/#/cleaner`.
 - The title and tagline on the splash page can be customized through the `KITE_TITLE` and
   `KITE_TAGLINE` variables in `.kiterc`.
 - A large number of stability and performance improvements.
 - tags.journal files will not grow large anymore.

### 1.4.4

 - Faster loading of the project list page.
 - Fixed missing CSS.

### 1.4.3

 - History editing improvements. Operations with problems (such as importing a file that no
   longer exists) can be edited now. Long histories can now be edited without problem. The
   UI has been revised a bit. (The _Validate_ button has been removed.)
 - Switching to Spark 1.4.0.
 - New operation _Copy graph into a segmentation_ can import the project as its own
   segmentation and create edges between the original vertices and
   their corresponding vertices in the segmentation.
 - Operations _Degree_, _Aggregate on neighbors_, and _Weighted aggregate on neighbors_ can
   now also make calculations directly on unique neighboring vertices (that is, regardless of the
   number of edges between the vertices).
 - For neighbor aggregations and degree the meaning of "symmetric" edges makes more sense now:
   the number of symmetric edges between A and B is now understood as
   _max(number of edgex A->B, number of edges B->A)_
 - EC2 Kite instances now listen on port 4044 (instead of 5080 before).
 - Smoke test script added: when you install a new Kite instance, you can run
     `kite_xxx/tools/daily_test.sh`
   to see if everything is set up correctly.
 - Fixed saving workflows. The save dialog didn't actually show in prod deployments.
 - Fixed a bug where we didn't close some files when we didn't need them which caused s3 lockups.
 - Appending data to an existing DB table is not supported anymore (as it's dangerous
   if done accidentally). In other words, you can only export to a DB by creating a new table.
 - Removed the _SQL dump_ option in file export operations. The only supported output format
   is CSV now.
 - Improved watchdog to detect a wider range of potential problems.
 - Fixed bug: editing the history now causes project reload.
 - Fixed a bug where vertices became frozen when attributes were visualized on them.
 - Fixed a bug where cross edges between project and segmentation could be broken for certain
   operations.

### 1.4.2

 - Improvements to the import graph workflow: somewhat less computation stages plus the
   algorithm is not sensitive anymore to vertices with millions of edges.
 - Two new attribute operations implemented: _Fill edge attribute with constant default value_
   and _Merge two edge attributes_. These do the same to edges what
   _Fill with constant default value_ and _Merge two attributes_ could do to vertices. Note that
   the latter two operations have been renamed to
   _Fill vertex attribute with constant default value_ and _Merge two vertex attributes_,
   respectively.
 - Lin's Centrality algorithm is added.

### 1.4.1

 - Fixed regression. (Segmentations opened on the wrong side.)

### 1.4.0

 - You can now omit columns when importing something from a CSV file.
 - Moderate performance improvements for graph visualization.
 - The _User's Guide_ contents are now integrated into LynxKite. Relevant help messages are
   provided through contextual popups, and the document is also available as a single page,
   ready for printing, through a new link at the bottom right.
 - New edge operation _"Merge parallel edges by attribute"_ makes it possible
   for the user to merge those parallel edges between two vertices that have the
   same value for the given edge attribute.
 - Admins can download the last server log using the link `http://<kite ip>:<kite port>/logs`.
 - When running an EC2 cluster, you cannot directly reference s3 files as before (using
   the format `s3n://AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY@bucket/path`), see the changelog
   entry below about the data file prefix notation. Instead, for EC2 cluster we automatically
   setup the prefix `S3` to point to `s3n://AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY@`. In practice
   this means that when using an EC2 cluster you need to refer to files on S3 as: `S3$bucket/path`.
 - Improved stability and graceful degradation. Not having enough memory now will only result in
   degraded performance not failures.
 - _"Create scale-free random edge bundle"_ operation added which allows one to create
   a scale free random graph.
 - One can save a sequence of operations as a workflow. The feature is accessible from the
   project history editor/viewer and saved workflows show up as a new operation category.
 - Strings visualized as icons will be matched to neutral icons (circle, square,
   triangle, hexagon, pentagon, star) if an icon with the given name does not exist.
 - _"PageRank"_ operation can now be used without weights.
 - Data files and directories can now only be accessed via a special prefix notation.
   For example, what used to be `hdfs://nameservice1:8020/user/kite/data/uploads/file` is
   now simply `UPLOAD$/file`. This enables the administrator to hide s3n passwords from
   the users; futhermore, it will be easier to move the data to another location. A new kiterc
   option `KITE_PREFIX_DEFINITIONS` can be used to provide extra prefixes (other
   than `UPLOAD$`) See the files `kiterc_template` and `prefix_definitions.txt` in directory
   `kite_???/conf` for details.
 - New aggregation method: `count distinct`
 - LynxKite pages can now be printed. Visualizations are also supported. This provides a
   method of exporting the visualization in a scalable vector graphics format.
 - Segmentation coverage is automatically calculated.
 - New vertex operation _"Centrality"_ makes it possible to count approximate harmonic
   centrality values.

### 1.3.1

 - Edges can now be colored based on string attributes as well.
 - SQL operations should be more portable now and will also work if field names contain
   special characters.
 - One can now use the newly added .kiterc option, `KITE_EXTRA_JARS`, to add JARS on the classpath
   of all LynxKite components. Most important usecase is to add JDBC drivers.
 - Multiple ways of 2D graph rendering.
 - Improved compatibility for Mozilla Firefox.
 - Show all visualized attributes as part of the legend in sampled view.

### 1.3.0

 - New vertex operation _"Add rank attribute"_ makes it possible for the user to sort by
   an attribute (of double type). This can be used to identify vertices which have the
   highest or lowest values with respect to some attributes.
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
 - Visualizations can be saved into the project for easier sharing.

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
