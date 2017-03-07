<!---
Please add changes to "master", preferably ordered by their significance. (Most significant on top.)
-->

# Changes

### master

### 1.13.0

 - Visualization settings UI now matches the new attribute UI.
 - When you try to overwrite a table or view, you now get a confirmation prompt instead of an error.
 - Tab-completion for LynxKite operations works in Python command-line
   and in Jupyter notebooks.
 - You can now filter for position (Double, Double) attributes.
 - Added _Segment by geographical proximity_ operation to segment vertices using Shapefiles and
   coordinate vertex attributes. Can be used e.g. to segment by geo regions or points of interests.
 - New _"Sample graph by random walks"_ operation added. It can be used to make small smart samples
   that preserves the structure of the network to speed up model creation.
 - Edge and scalar attribute notes added or augmented.
 - Merge vertices by attributes operation keeps links with base project.

### 1.12.1

 - Admins can backup LynxKite project data from UI.
 - Column types are displayed in SQL result box.
 - Added _Lookup Region_ operation for finding locations inside shapefiles.

### 1.12.0

 - Restore Hive support. (It was unintentionally disabled with the Spark upgrade in
   LynxKite 1.11.0.)
 - Remote API works on PizzaKite now.
 - Added a table and column explorer to the SQL box. Click on the "Tables" button in the SQL
   box to give it a try!
 - Upgraded to _Apache Spark 2.1.0_.
 - You can assign icons to attributes/segmentations/scalars. (As suggested by Marton Szel. :+1:)
 - New _"Copy scalar from other project"_ operation added. For example it can be used to take a model which
   was trained in an other project.
 - Clicking on a table or view scrolls to the global SQL box and executes the
   `select *` query on it.
 - You can submit SQL queries with Ctrl-Enter.
 - New Remote API methods: `project.copy()`, `project.global_table_name()`,
   `project.vertices_table()`, `project.edges_table()`.
 - Spark Monitor doesn't send alarm in case of long-running tasks.

Ecosystem:

 - Replace `run_task.sh` and `show_target.sh` with a new unified `tasks.sh`.
 - Renamed from "Lynx Data Automation Framework" to "Lynx Enterprise".

### 1.11.0

LynxKite:

 - Upgraded to _Apache Spark 2.0.2_.
 - The Spark 2.x upgrade involved migration to the new Spark machine learning API. This will have
   a positive impact in the long run, but for now the effects are:
    - _Logistic regression_ in the _"Predict from vertex attributes"_ operation becomes a binary
      classifier.
    - _"Train a k-means clustering model"_ no longer automatically scales the selected attributes.
      This is actually helpful if the attributes are on the same scale already, but otherwise you
      need to manually scale them now.
    - Previously trained k-means models will give wrong results because scaling has been removed.
      You will have to train new models.
 - Fixed all reported bugs in the new project UI.
 - Vertex and edge ID attribute values are only optionally unique in "_Import vertex attributes_"
   and "_Import edge attributes_" operations.
 - New "_Split Edges_" operation added. It can be used to create multiple copies of the same edge,
   based on a repetition attribute.
 - "_Derive vertex attribute_" and "_Derive edge attribute_" operations can be evaluated on all
   vertices / edges using undefined input attribute values.
 - "_Derive vertex attribute_" and "_Derive edge attribute_" operations can return vectors of
   doubles or vectors of strings.

### 1.10.0

LynxKite:

 - More compact UI for scalars and attributes.
 - Fill full width if only one project is open.
 - New _"Count most common"_ local aggregator added.
 - Scalable _"Count distinct"_ and _"Most common"_ aggregators.
 - PageRank and Centrality algorithms have an edge direction parameter.

### 1.9.6

LynxKite:

 - Performance and scalability improvements for aggregating operations.
 - Cleaner cleaner UI. (The UI for cleaning up old data files to recover storage space.
   Apologies for the pun.)
 - SQL boxes now persist their query history into the browser's local storage.
 - SQL box has _"Show more"_ button to easily look at more results.
 - Assorted UI fixes.
 - Views can now be edited. Views and tables exported from the global sql box can also be edited.
 - Scalable approximate embeddedness operation added.
 - Scalable approximate clustering coefficient operation added.
 - History view can now generate Python code. (Useful for ecosystem task authors.)
 - Various visualization limits (such as the maximum 10,000 edges) can be adjusted with the
     `KITE_DRAWING_OVERALL` setting (default 10,000) and its more fine-grained companions.
 - Scalability improvement for Merge vertices and Import segmentation operations.
 - New _"Grow segmentation"_ operation added.

Ecosystem:

 - Chronomaster user interface can tell you about past and future tasks.
 - Flexible date template strings.
 - Task repetition is now specified in calendar-supporting Cron format instead of fixed-length time
   intervals.
 - Segmentations and attribute histograms in Python Remote API.

### 1.9.5.4

Ecosystem:

 - Extra date template strings `{to,yester}day_{yyyy,mm,dd}`.
 - Fix Chronomaster metrics reporting when `num_workers` is greater than one.
 - Work around Scala 2.10 reflection thread safety issue. (SI-6240)

### 1.9.5.3

Ecosystem:

 - Add extra date template strings, like `yesterday_minus_1month_mm_str`.

### 1.9.5.2

Fixes a bug in the HDFS transfer performance improvement in 1.9.5.1.

### 1.9.5.1

Cherry-pick release with just the changes below.

Ecosystem:

 - Much improved `TransferTask` performance for HDFS to HDFS copy. (Up to 20 times faster in some
   cases.)
 - `count` feature in `helper.sh` to help with verification testing.
 - Correct Graphite configuration for Spark executors to avoid massive error logging and collect
   more detailed performance data.

### 1.9.5

 - Relax the Luigi API Python version requirement from 3.5+ to 3.4+.

### 1.9.4

LynxKite:

 - SQL boxes can now show as many rows as you like.
 - History page performance significantly improved.
 - SQL box on the project browser can be opened more intuitively.
 - User passwords and admin status can be changed from the UI by administrators.
 - LDAP authentication.

Ecosystem:

 - Node-level monitoring.
 - Flexible transfers, including to secured HDFS clusters.
 - Support for unusual DB2 features.
 - `no_repeat` option added to Chronomaster configuration.
 - Tasks can be constrained to time windows.
 - New Luigi task types for creating tables and projects.

### 1.9.3

 - Assorted ecosystem bugfixes and improvements
 - Dockerless installation option :(
 - New operation _"Enumerate triangles"_ has been created.

### 1.9.2

 - New operation _"Triadic closure"_ has been created.
 - New operation _"Create snowball sample"_ has been created.
 - Internal watchdog in ecosystem mode.

### 1.9.1

 - Fix startup script to support RHEL 6.
 - A new machine learning operations category is created and added to the toolbox.
 - SQL-related bug fixes.

### 1.9.0

 - Global SQL-box has been added.
 - Views have been added to avoid expensive serialization to tables when it is not required.
 - Import options are now stored for each table and can be reused via "Edit import" button
 - New operation _"Hash vertex attribute"_ has been added.
 - Discarded projects and tables are moved to _Trash_ instead of immediate permanent deletion.
 - JDBC import can now use `VARCHAR` columns as partitioning keys. For small tables the key can even
   be omitted.
 - New operations _"Train a Logistic regression model"_, _"Classify with a model"_, _"Train a k-means
   clustering model"_, and _"Reduce vertex attributes to two dimensions"_ have been added.
 - Statistics are displayed on the linear regression models.

### 1.8.0

 - Major performance and scalability improvements.
 - New option has been added in bucketed view: relative edge density.
 - New operation _"Find vertex coloring"_ has been added.
 - Add search box to built-in help.
 - Experimental feature: LynxKite can be used from [Jupyter](http://jupyter.org) (IPython Notebook).

### 1.7.5

 - Improve speed and stability of the project history editor
 - Easier to find data export button
 - Table import wizard: added tab stops and removed highlight flickering
 - Hide ACL settings in single-user instances
 - New aggregator: `median`.

### 1.7.4

 - Prefix-based user access control can be specified in the prefix definitions file.
 - emr.sh: support setting up LynxKite in an Amazon VPC.
 - Numeric fields in CSV files can be imported to LynxKite with the right types.
 - When importing edges for existing vertices, you can now join by non-string attributes too.
 - Fixed batch scripting issue with `lynx.loadProject()`.
 - Bottom links are moved to a popup.
 - Support for Kerberos-secured clusters.
 - Attribute filter `*` added to match all defined values. This can be used e.g. to remove
   vertices with no location from a map visualization.
 - Stability improvements regarding edge loading and handling graphs with large degree vertices.
 - SQL query results can be saved as segmentations.
 - Prettier project history editor.

### 1.7.3

 - New configuration option: `KITE_INSTANCE` added; this should be a string identifying the
   instance (e.g., MyClient). It is strongly recommended that you set it at installation: it
   will be used to identity the cluster in logs.
 - Changes in vertex and edge count after an operation are reported on the UI.
 - Fixed data export in Amazon EMR.
 - Fixed _Import JDBC table_ button.
 - `emr.sh` can now handle non-default region.

### 1.7.2

 - SQL query results can be sorted by clicking the column headers.
 - Fixed visualization save/restore, now for real, we promise.
 - When visualizing two graphs, a separating line is displayed between them.

### 1.7.1

 - History can be accessed even if some operations no longer exist. (1.7.0 removed all classical
   import operation, so this is an important fix.)
 - Improve scalability and performance of the Centrality algorithm family.
 - Fixed saved visualizations, which were broken in 1.7.0.
 - LynxKite log directory can now be configured. (`KITE_LOG_DIR`)
 - All attribute types are now accessible through the SQL interface. Types not supported by SQL will
   be presented as strings.
 - Improved security: JSON queries are only accepted with the `X-Requested-With: XMLHttpRequest`
   header.
 - Compressed files can be uploaded and handled as if they were not compressed. (Supported
   extensions are `.gz`, `.bz2`, `.lzo`, and `.snappy`. Compressed files accessed from HDFS were
   always supported.)
 - Improved error messages and documentation.
 - Case insensitive project search.
 - New operation, _Internal edge ID as attribute_.

### 1.7.0

 - Major changes to importing and exporting data. We introduce the concept of tables to improve
   clarity and performance when working with external data.

   Projects no longer depend on the input files. (They can be deleted after importing.) It becomes
   easier to share raw data between projects. We have fewer operations (just _Import vertices from
   table_ instead of _Import vertices from CSV files_ and _Import vertices from database_), but
   support more formats (JSON, Parquet and ORC are added) with a unified interface. We
   also support direct import from Hive.

   Tables are built on Apache Spark DataFrames. As a result, you can run SQL queries on graphs. (See
   the SQL section at the bottom of a project.) Plus DataFrame-based data manipulation is now
   possible from Groovy scripts.

   Export operations are gone. Data can be exported in various formats through the SQL interface.
   SQL results can also be saved as tables and re-imported as parts of a project.

   For more details about the new features see the documentation.
 - Default home directory is moved under the 'Users' folder.
 - Root folder is default readable by everyone and writable by only admin users for
   bare new Kite installations.
 - Edges and segmentation links can now also be accessed as DataFrames from batch scripts.
 - New _Derive scalar_ operation.
 - Possible to create visualizations with lighter Google Maps as a background thanks to
   adjustable map filters.
 - Upgrade to Hadoop 2 in our default Amazon EC2 setup.
 - Remove support of Hadoop 1.
 - Introduce `tools/emr.sh` which starts up an Amazon Elastic MapReduce cluster. This is
   now the recommended way to run Kite clusters on Amazon.
 - Introduce operation _Copy edges to base project_.
 - `emr.sh` can now invoke groovy scripts on a remote cluster.
 - Introduce explicit machine learning models. Create them with the _Train linear regression model_
   operation and use them for predictions with _Predict from model_.
 - Added a new centrality measure, the _average distance_.
 - The _Convert vertices into edges_ operation has been removed. The same functionality is now
   available via tables. You can simply import the `vertices` table of one project as edges in
   another project.

### 1.6.1.2

 - Fixed edge attribute import.

### 1.6.1.1

 - Fixed a critical performance regression with derived attributes.
 - Fixed issues with _Predict vertex attribute_ operation.
 - Switched `spark.io.compression.codec` from `snappy` to `lz4`.
 - Added extra logging for health check failures.

### 1.6.1

 - Fixed critical bugs with importing files.
 - Upgraded to Apache Spark 1.6.0.
 - Project directories now have access control (can be private or public), just like projects.
 - Operation _Modular clustering_ stops crashing.
 - The project browser now remembers the last directory you browsed.
 - `run-kite.sh start` now waits for the web server to be initialized - previously it returned
   immediately after starting the initialization process. This may take minutes on
   certain instances, but at least you know when the server is ready.
 - A home folder is created for every user automatically. Every user has exclusive read and write
   access to his own home folder by default.
 - The JavaScript code can now access scalars.
 - If an attribute has already been calculated, a small checkmark indicates this.
 - Fixed critical bug with batch workflows.

### 1.6.0

 - One can now run batch workflows on a running Kite using Ammonite. Just SSH into the ammonite port
   and do `batch.runScript(...)`. See Ammonite's welcome message for details.
 - Fingerprinting between project and segmentation made more general: it can now add new
   connections, not only select from existing ones.
 - Reintroduced project search on the project selector UI.
 - Added the _Predict vertex attribute_ operation to offer some simple machine learning tools.
 - Significantly reduced chance of out of memory errors in LynxKite. (We do not require anymore
   that any spark data partition has to fit in memory.)
 - Long attributes can no longer be referenced in derived attributes. This is to avoid the
   surprising rounding that JavaScript performs on them. The attributes have to be converted to
   strings or doubles first.
 - Created new operations _Add random vertex attribute_ and _Add random edge attribute_. You can
   specify the desired distribution (normal or uniform). These new operations obsolete _Add gaussian
   vertex attribute_, which is no longer accessible from the operations menu.
 - New operations to support finding co-locations: _Sample edges from co-occurrence_,
   _Segment by event sequence_.
 - Enable creating segmentations on top of other segmentations.
 - Batch scripts can now access projects and files as Apache Spark DataFrames and run SQL queries
   on them.
 - Workflows and batch scripts can run workflows.
 - Added new operation: _Pull segmentation one level up_.
 - Improved center picker experience.

### 1.5.8

 - Fixed a critical issue with health checking.

### 1.5.7

 - New aggregator added for all non-global aggregation operations: you can collect all unique
   values into a set.
 - A new operation _Split vertices_ was added. It can be used to create multiple copies
   of the same vertex, based on a 'count' attribute.
 - You can use Ammonite (https://lihaoyi.github.io/Ammonite/#Ammonite-REPL) to interact with a
   running Kite via a Scala interpreter. Among others, this allows one to use sparkSQL for
   arbitrary data processing tasks using the computation resources held by Kite. For details on
   how to set this up, see `conf/kiterc_template`.
 - Added the _Shortest path_ operation for computing the shortest path from a given set of vertices.
 - Added the _Copy edges to segmentation_ operation for translating the edges of the base project
   into a segmentation.
 - Added _Import segmentation_ operations.
 - _Merge vertices by attribute_ will no longer discard segmentations.
 - Added _Segment by interval_ operation.
 - Until now we replaced spaces with underscores in project names and did the reverse when
   displaying them. We now stop doing this to avoid some inconsistencies. (Feel free to use either
   underscores or spaces in project names. Both work fine.)
 - Add possibility to edit workflows.
 - Show the number of edges that will be created before executing the _Created edges from co-occurrence_
   operation.

### 1.5.6

 - Upgraded to Spark 1.5.1.

### 1.5.5

 - Fixed memory issue with history requests.

### 1.5.4

 - Operations _Export edge attributes to file_ and _Export segmentation to file_ now support
   vertex attribute parameters with which the user can specify how the vertices connected by
   the edges will be identified in the output file.
 - Precise histogram computations: so far histograms were computed using a sample of the real
   data, for performance reasons. Now the user has the option to request a computation on all
   the data.

### 1.5.3

 - Fixed bug where the root project directory was not accessible for non-admins.
 - Slow computations will not lock up the browser anymore. Previously it was possible that if too
   many slow computations were requested then even fast operations, like opening a project stopped
   working.
 - HTTP security and performance improvements.
 - Kite default port changed from 9000 to 2200. This does not impact you if you already have a
   `.kiterc` file with whatever port number set, it just changes the template.
 - Fixed bug where an edge might have been colored when the color by attribute was not even defined.
 - Removed not implemented relative edge weight parameter from fingerprinting operations.

### 1.5.2

 - Automated end-to-end testing - basically a robot clicking all around on a test Kite instance -
   made part of the development workflow and lots of tests added.
 - Dispersion computation made significantly faster.
 - Projects can be organized into folders now.
 - Migration to 1.5.x does not take a lot of memory anymore.
 - Summary information of how things were created is presented on the UI. For example, the formula
   used to create a derived attribute will accompany it. (No full coverage yet.)
 - Logarithmic histograms support non-positive values.
 - New parameter for CSV import operations: `Tolerate ill-formed lines`. It controls
   whether or not non-conforming lines in the csv file should be skipped silently
   or cause an error immediately.
 - Further sample vertices can be picked by clicking _"Pick"_ and then _"Next"_ in the advanced
   pick options.
 - If the user requests a visualization that's too large (and would probably kill the browser)
   we return an error instead of trying to display the large visualization.
 - Users can now import additional attributes from CSV/SQL for edges as well (until now, it was
   only possible for vertices).
 - The internal identification of LynxKite meta graph entities depended on Java's default character
   encoding which in turn depended on the terminal setting of the user starting LynxKite. This resulted
   in a LynxKite failure if a user with different settings restarted the server. Now this is fixed by
   always using UTF-8 encoding. But this will cause problems if your LynxKite instance uses something
   different (UTF-8 seems to be typical, though). If LynxKite fails with "cannot load project list"
   and you see and error in the logs starting with "Output mismatch on ...", then try to force a
   migration: `KITE_FORCED_MIGRATION=true  ./run-kite-....sh restart`. Do this only once, not for
   all restarts in the future!
 - Copy graph into segmentation operation fixed.
 - Edge attributes are included when copying visualizations to the clipboard.

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
