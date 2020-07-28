''' Python documentation for the operations in Lynxkite.
This document has been automatically generated.
'''


def growSegmentation(*args, **kwargs):
  '''  Grows the segmentation along edges of the parent graph.

  This operation modifies this segmentation by growing each segment with the neighbors of its elements.
  For example if vertex A is a member of segment X and edge A{to}B exists in the original graph
  then B also becomes the member of X (depending on the value of the direction parameter).

  This operation can be used together with <<Use base graph as segmentation>> to create a
  segmentation of neighborhoods.

  :param direction:   Adds the neighbors to the segments using this direction.
  '''


def exportToOrc(*args, **kwargs):
  '''  https://orc.apache.org/[Apache ORC] is a columnar data storage format.

  :param path:   The distributed file-system path of the output file. It defaults to `<auto>`, in which case the
    path is auto generated from the parameters and the type of export (e.g. `Export to CSV`).
    This means that the same export operation with the same parameters always generates the same path.
  :param version:   Version is the version number of the result of the export operation. It is a non negative integer.
    LynxKite treats export operations as other operations: it remembers the result (which in this case
    is the knowledge that the export was successfully done) and won't repeat the calculation. However,
    there might be a need to export an already exported table with the same set of parameters (e.g. the
    exported file is lost). In this case you need to change the version number, making that parameters
    are not the same as in the previous export.
  :param for_download:   Set this to "true" if the purpose of this export is file download: in this case LynxKite will
    repartition the data into one single file, which will be downloaded. The default "no" will
    result in no such repartition: this performs much better when other, partition-aware tools
    are used to import the exported data.

    include::{g}
  '''


def reduceAttributeDimensions(*args, **kwargs):
  '''  Transforms (embeds) a `Vector` attribute to a lower-dimensional space.
  This is great for laying out graphs for visualizations based on vertex attributes
  rather than graph structure.

  :param save_as:   The new attribute will be created under this name.
  :param vector:   The high-dimensional vertex attribute that we want to embed.
  :param dimensions:   Number of dimensions in the output vector.
  :param method:   The dimensionality reduction method to use.
    https://en.wikipedia.org/wiki/Principal_component_analysis
  :param perplexity:   Size of the vertex neighborhood to consider for t-SNE.
  '''


def importSnapshot(*args, **kwargs):
  '''  Makes a previously saved snapshot accessible from the workspace.

  :param path:   The full path to the snapshot in LynxKite's virtual filesystem.
  '''


def aggregateEdgeAttributeGlobally(*args, **kwargs):
  '''  Aggregates edge attributes across the entire graph into one graph attribute for each attribute.
  For example you could use it to calculate the average call duration across an entire call dataset.

  :param prefix:   Save the aggregated values with this prefix.

    include::{g}
  '''


def discardGraphAttributes(*args, **kwargs):
  '''  Throws away graph attributes.

  :param name:   The graph attributes to discard.
  '''


def splitEdges(*args, **kwargs):
  '''  Split (multiply) edges in a graph. A numeric edge attribute controls how many
  copies of the edge should exist after the operation. If this attribute is
  1, the edge will be kept as it is. If this attribute is zero, the edge
  will be discarded entirely. Higher values (e.g., 2) will result in
  more identical copies of the given edge.

  After the operation, all previous edge attributes will be preserved;
  in particular, copies of one edge will have the same values for the previous edge
  attributes. A new edge attribute (the so called index attribute) will also be
  created so that you can differentiate between copies of the same edge.
  If a given edge was multiplied by n times, the n new edges will have n different
  index attribute values running from 0 to n-1.

  :param rep:   A numeric edge attribute that specifies how many copies of the edge should
    exist after the operation.
    (The value is rounded to the nearest integer, so 1.8 will mean 2 copies.)
  :param idx:   The name of the attribute that will contain unique identifiers for the otherwise
    identical copies of the edge.
  '''


def mergeParallelSegmentationLinks(*args, **kwargs):
  '''  Multiple segmentation links going from A base vertex to B segmentation vertex
  will be merged into a single link.

  After performing a <<merge-vertices-by-attribute, Merge vertices by attribute>> operation, there might
  be multiple parallel links going between some of the base graph and segmentation vertices.
  This can cause unexpected behavior when aggregating to or from the segmentation.
  This operation addresses this behavior by merging parallel segmentation links.


  '''


def replaceWithEdgeGraph(*args, **kwargs):
  '''  Creates the http://en.wikipedia.org/wiki/Edge_graph[edge graph] (or line graph),
  where each vertex corresponds to an edge in the current graph.
  The vertices will be connected, if one corresponding edge is the continuation of the other.


  '''


def copyEdgeAttribute(*args, **kwargs):
  '''  Creates a copy of an edge attribute.

  :param from:
  :param to:
  '''


def copyEdgesToSegmentation(*args, **kwargs):
  '''  Copies the edges from the base graph to the segmentation. The copy is performed along the links
  between the base graph and the segmentation. If a base vertex belongs to no segments, its edges
  will not be found in the result. If a base vertex belongs to multiple segments, its edges will
  have multiple copies in the result.


  '''


def computeDistanceViaShortestPath(*args, **kwargs):
  '''  Calculates the length of the shortest path from a given set of vertices for every vertex.
  To use this operation, a set of starting _v~i~_ vertices has to be specified, each with
  a starting distance _sd(v~i~)_. Edges represent a unit distance by default, but this
  can be overridden using an attribute. This operation will compute for each vertex
  _v~i~_ the smallest distance from a starting vertex, also counting the starting
  distance of the starting vertex: _d(v~i~)

  :param name:   The new attribute will be created under this name.
  :param edge_distance:   The attribute containing the distances corresponding to edges. (Cost in the above example.)

    Negative values are allowed but there must be no loops where the sum of
    distances is negative.
  :param starting_distance:   A numeric attribute that specifies the initial distances of the vertices that we
    consider already reachable before starting this operation. (In the above example,
    specify this for the elements of the starting set, and leave this undefined for
    the rest of the vertices.)
  :param iterations:   The maximum number of edges considered for a shortest-distance path.
  '''


def findInfocomCommunities(*args, **kwargs):
  '''  Creates a segmentation of overlapping communities.

  The algorithm finds maximal cliques then merges them to communities.
  Two cliques are merged if they sufficiently overlap.
  More details can be found in
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id

  :param cliques_name:   A new segmentation with the maximal cliques will be saved under this name.
  :param communities_name:   The new segmentation with the infocom communities will be saved under this name.
  :param bothdir:   Whether edges have to exist in both directions between all members of a clique.
    +
  :param min:   Cliques smaller than this will not be collected.
    +
    This improves the performance of the algorithm, and small cliques are often not
    a good indicator anyway.
  :param adjacency_threshold:   Clique overlap is a measure of the overlap between two cliques relative to
    their sizes. It is normalized to
  '''


def makeAllSegmentsEmpy(*args, **kwargs):
  '''  Throws away all segmentation links.


  '''


def sql5(*args, **kwargs):
  '''  Executes an SQL query on its five inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`, `four`, `five`. For example:

  ```
  select * from one
  union select * from two
  union select * from three
  union select * from four
  union select * from five
  ```

  :prefix: one.
  :maybe-tick: {backtick}
  include::{g}[tag


  '''


def mergeTwoEdgeAttributes(*args, **kwargs):
  '''  An attribute may not be defined on every edge. This operation uses the secondary
  attribute to fill in the values where the primary attribute is undefined. If both are
  undefined on an edge then the result is undefined too.

  :param name:   The new attribute will be created under this name.
  :param attr1:   If this attribute is defined on an edge, then its value will be copied to the output attribute.
  :param attr2:   If the primary attribute is not defined on an edge but the secondary attribute is, then the
    secondary attribute's value will be copied to the output variable.
  '''


def sql4(*args, **kwargs):
  '''  Executes an SQL query on its four inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`, `four`. For example:

  ```
  select * from one
  union select * from two
  union select * from three
  union select * from four
  ```

  :prefix: one.
  :maybe-tick: {backtick}
  include::{g}[tag


  '''


def importParquet(*args, **kwargs):
  '''  https://parquet.apache.org/[Apache Parquet] is a columnar data storage format.

  :param filename:   The distributed file-system path of the file. See <<prefixed-paths>> for more details on specifying
    paths.

    include::{g}
  '''


def takeSegmentationAsBaseGraph(*args, **kwargs):
  '''  Takes a segmentation of a graph and returns the segmentation as a base graph itself.


  '''


def exportToParquet(*args, **kwargs):
  '''  https://parquet.apache.org/[Apache Parquet] is a columnar data storage format.

  :param path:   The distributed file-system path of the output file. It defaults to `<auto>`, in which case the
    path is auto generated from the parameters and the type of export (e.g. `Export to CSV`).
    This means that the same export operation with the same parameters always generates the same path.
  :param version:   Version is the version number of the result of the export operation. It is a non negative integer.
    LynxKite treats export operations as other operations: it remembers the result (which in this case
    is the knowledge that the export was successfully done) and won't repeat the calculation. However,
    there might be a need to export an already exported table with the same set of parameters (e.g. the
    exported file is lost). In this case you need to change the version number, making that parameters
    are not the same as in the previous export.
  :param for_download:   Set this to "true" if the purpose of this export is file download: in this case LynxKite will
    repartition the data into one single file, which will be downloaded. The default "no" will
    result in no such repartition: this performs much better when other, partition-aware tools
    are used to import the exported data.

    include::{g}
  '''


def weightedAggregateEdgeAttributeToVertices(*args, **kwargs):
  '''  Aggregates an attribute on all the edges going in or out of vertices.
  For example it can calculate the average cost per second of calls for each person.

  :param prefix:   Save the aggregated attributes with this prefix.
  :param weight:   The `number` attribute to use as weight.
  :param direction:   - `incoming edges`: Aggregate across the edges coming in to each vertex.
     - `outgoing edges`: Aggregate across the edges going out of each vertex.
     - `all edges`: Aggregate across all the edges going in or out of each vertex.

    include::{g}
  '''


def hashVertexAttribute(*args, **kwargs):
  '''  Uses the https://en.wikipedia.org/wiki/SHA-256[SHA-256] algorithm to hash an attribute: all values
  of the attribute get replaced by a seemingly random value. The same original values get replaced by
  the same new value and different original values get (almost certainly) replaced by different new
  values.

  Treat the salt like a password for the data. Choose a long string that the recipient of the data has
  no chance of guessing. (Do not use the name of a person or project.)

  The salt must begin with the prefix `SECRET(` and end with `)`, for example
  `SECRET(qCXoC7l0VYiN8Qp)`. This is important, because LynxKite will replace such strings with
  three asterisks when writing log files. Thus, the salt cannot appear in log files.  Caveat: Please
  note that the salt must still be saved to disk as part of the workspace; only the log files are
  filtered this way.

  To illustrate the mechanics of irreversible hashing and the importance of a good salt string,
  consider the following example. We have a data set of phone calls and we have hashed the phone
  numbers. Arthur gets access to the hashed data and learns or guesses the salt. Arthur can now apply
  the same hashing to the phone number of Guinevere as was used on the original data set and look her
  up in the graph. He can also apply hashing to the phone numbers of all the knights of the round
  table and see which knight has Guinevere been making calls to.

  :param attr:   The attribute(s) which will be hashed.
  :param salt:   The value of the salt.
  '''


def copyEdgesToBaseGraph(*args, **kwargs):
  '''  Copies the edges from a segmentation to the base graph. The copy is performed along the links
  between the segmentation and the base graph. If two segments are connected with some
  edges, then each edge will be copied to each pairs of members of the segments.

  This operation has a potential to create trillions of edges or more.
  The number of edges created is the sum of the source and destination segment sizes multiplied
  together for each edge in the segmentation.
  It is recommended to drop very large segments before running this computation.


  '''


def exportToJson(*args, **kwargs):
  '''  JSON is a rich human-readable data format. It produces larger files than CSV but can represent
  data types. Each line of the file stores one record encoded as a
  https://en.wikipedia.org/wiki/JSON[JSON] object.

  :param path:   The distributed file-system path of the output file. It defaults to `<auto>`, in which case the
    path is auto generated from the parameters and the type of export (e.g. `Export to CSV`).
    This means that the same export operation with the same parameters always generates the same path.
  :param version:   Version is the version number of the result of the export operation. It is a non negative integer.
    LynxKite treats export operations as other operations: it remembers the result (which in this case
    is the knowledge that the export was successfully done) and won't repeat the calculation. However,
    there might be a need to export an already exported table with the same set of parameters (e.g. the
    exported file is lost). In this case you need to change the version number, making that parameters
    are not the same as in the previous export.
  :param for_download:   Set this to "true" if the purpose of this export is file download: in this case LynxKite will
    repartition the data into one single file, which will be downloaded. The default "no" will
    result in no such repartition: this performs much better when other, partition-aware tools
    are used to import the exported data.


    include::{g}
  '''


def deriveGraphAttribute(*args, **kwargs):
  '''  Generates a new <<graph-attributes, graph attribute>> based on existing graph attributes.
  The value expression can be an arbitrary Scala expression, and it can refer to existing
  graph attributes as if they were local variables.

  For example you could derive a new graph attribute as `something_sum / something_count` to get the average
  of something.

  :param output:   The new graph attribute will be created under this name.
  :param expr:   The Scala expression. You can enter multiple lines in the editor.
  '''


def discardVertexAttributes(*args, **kwargs):
  '''  Throws away vertex attributes.

  :param name:   The vertex attributes to discard.
  '''


def addConstantVertexAttribute(*args, **kwargs):
  '''  Adds an attribute with a fixed value to every vertex.

  :param name:   The new attribute will be created under this name.
  :param value:   The attribute value. Should be a number if _Type_ is set to `number`.
  :param type:   The operation can create either `number` or `String` typed attributes.
  '''


def filterByAttributes(*args, **kwargs):
  '''  Keeps only vertices and edges that match the specified filters.

  You can specify filters for multiple attributes at the same time, in which case you will be left
  with vertices/edges that match all of your filters.

  Regardless of the exact the filter, whenever you specify a filter for an attribute you always
  restrict to those edges/vertices where the attribute is defined. E.g. if say you have a filter
  requiring age > 10, then you will only keep vertices where age attribute is defined and the value of
  age is more than ten.

  The filtering syntax depends on the type of the attribute in most cases.

  [p-ref1]#Match all filter#::
  For every attribute type `*` matches all defined values. This is useful for discarding
  vertices/edges where a specific attribute is undefined.

  [p-ref2]#Comma separated list#::
  This filter is a comma-separated list of values you want to match. It can be used for
  `String` and `number` types. For example `medium,high` would be a String filter
  to match these two values only, e.g., it would exclude `low` values. Another example is `19,20,30`.

  [p-ref3]#Comparison filters#::
  These filters are available for `String` and `number` types.
  You can specify bounds, with the `<`, `>`, `&lt;

  :param ref1:   For every attribute type `*` matches all defined values. This is useful for discarding
    vertices/edges where a specific attribute is undefined.
  :param ref2:   This filter is a comma-separated list of values you want to match. It can be used for
    `String` and `number` types. For example `medium,high` would be a String filter
    to match these two values only, e.g., it would exclude `low` values. Another example is `19,20,30`.
  :param ref3:   These filters are available for `String` and `number` types.
    You can specify bounds, with the `<`, `>`, `&lt;
  :param ref4:   For `String` and `number` types you can specify intervals with brackets.
    The parenthesis (`( )`) denotes an exclusive boundary
    and the square bracket (`
  :param ref5:   For `String` attributes, regex filters can also be applied. The following tips and examples
    can be useful:
    * `regex(xyz)` for finding strings that contain `xyz`.
    * `regex(^Abc)` for strings that start with `Abc`.
    * `regex(Abc$)` for strings that end with `Abc`.
    * `regex((.)\1)` for strings with double letters, like `abbc`.
    * `regex(\d)` or `regex(
  :param ref6:   For the `Vector
  :param ref7:   These filters can be used for attributes whose type is `Vector`.
    The filter `all(...)` will match the `Vector` only when the internal filter matches all elements of the
    `Vector`. You can also use `forall` and `Ɐ` as synonyms. For example `all(<0)` for a `Vector
  :param ref8:   Any filter can be prefixed with `!` to negate it. For example `!medium` will exclude
    `medium` values. Another typical usecase for this is specifying `!` (a single exclamation mark
    character) as the filter for a String attribute. This is interpreted as non-empty, so it will
    restrict to those vertices/edges where the String attribute is defined and its value is not empty
    string. Remember, all filters work on defined values only, so `!*` will not match any
    vertices/edges.
  :param ref9:   If you need a string filter that contains a character with a special meaning (e.g., `>`), use double quotes around
    the string. E.g., `>"
  '''


def findMaximalCliques(*args, **kwargs):
  '''  Creates a segmentation of vertices based on the maximal cliques they are the member of.
  A maximal clique is a maximal set of vertices where there is an edge between every two vertex.
  Since one vertex can be part of multiple maximal cliques this segmentation might be overlapping.

  :param name:   The new segmentation will be saved under this name.
  :param bothdir:   Whether edges have to exist in both directions between all members of a clique.
    +
  :param min:   Cliques smaller than this will not be collected.
    +
    This improves the performance of the algorithm, and small cliques are often not a good indicator
    anyway.
  '''


def createScaleFreeRandomEdges(*args, **kwargs):
  '''  Creates edges randomly so that the resulting graph is scale-free.

  This is an iterative algorithm. We start with one edge per vertex and in each
  iteration the number of edges gets approximately multiplied by
  _Per iteration edge number multiplier_.

  :param iterations:   Each iteration increases the number of edges by the specified multiplier.
    A higher number of iteration will result in a more scale-free degree distribution,
    but also a slower performance.
  :param periterationmultiplier:   Each iteration increases the number of edges by the specified multiplier.
    The edge count starts from the number of vertices, so with _N_ iterations and _m_
    as the multiplier you will have _m^N^_ edges by the end.
  '''


def useTableAsSegmentation(*args, **kwargs):
  '''  Imports a segmentation from a table. The table must have a column identifying an existing vertex by
  a String attribute and another column that specifies the segment it belongs to.
  Each vertex may belong to any number of segments.

  The rest of the columns in the table are ignored.

  :param name:   The imported segmentation will be created under this name.
  :param base_id_attr:   The `String` vertex attribute that identifies the base vertices.
  :param base_id_column:   The table column that identifies vertices.
  :param seg_id_column:   The table column that identifies segments.
  '''


def importNeo4j(*args, **kwargs):
  '''  Import data from an existing Neo4j database. The connection can be configured through the following
  variables in the .kiterc file:

  * `NEO4J_URI`: URI to connect to Neo4j, only bolt protocol is supported. The URI has to follow the
     `bolt://<host>:<port>` structure.
  * `NEO4J_PASSWORD`: Password to connect to Neo4j. You can leave it empty in case no password is required
  * `NEO4J_USER`: User used to connect to Neo4j

  In case you want to change the values of the variables, you will have to restart LynxKite for the
  changes to take effect.

  :param node_label:   The label for the type of node that you want to import from Neo4j. All the nodes with that label will be
    imported as a table, with each property as a column. You can specify the properties to import using the
    `Columns to import` parameter. The id ( `id()` function of Neo4j) of the node will be automatically included
    in the import as the special variable `id$`.
    Only one of node label or relationship type can be specified.
  :param relationship_type:   The type of the relationship that you want to import from Neo4j. The relationship will be imported
    as a table, with each property as a column. You can specify the properties to import using the
    `Columns to import` parameter.
    If you want to import properties from the source or the destination (target) nodes you can do it
    by adding the prefix `source_` or `target_` to the property. The id ( `id()` function of Neo4j) of
    both the source and the destination nodes, will be automatically included in the import as the special
    variables `source_id$` and `target_id$`.
    Only one of node label or relationship type can be specified.
  :param num_partitions:   LynxKite will perform this many queries in parallel to get the data. Leave at zero to let
    LynxKite automatically decide. Set a specific value if you want to control the level of
    parallelism.
  :param infer:   Automatically tries to cast data types from Neo4j. For example a column full of numbers will become a
    `number`. If disabled, all columns are imported as ``String``. It is recommended to set this to false,
    as Neo4j types do not integrate very well with Spark (Eg. Date types from Neo4j are not supported).

    include::{g}
  '''


def linkBaseGraphAndSegmentationByFingerprint(*args, **kwargs):
  '''  Finds the best matching between a base graph and a segmentation.
  It considers a base vertex A and a segment B a good "match"
  if the neighborhood of A (including A) is very connected to the neighborhood of B (including B)
  according to the current connections between the graph and the segmentation.

  The result of this operation is a new edge set between the base graph and the
  segmentation, that is a one-to-one matching.

  :param mo:   The number of common neighbors two vertices must have to be considered for matching.
    It must be at least 1. (If two vertices have no common neighbors their similarity would be zero
    anyway.)
  :param ms:   The similarity threshold below which two vertices will not be considered a match even if there are
    no better matches for them. Similarity is normalized to
  :param extra:   You can use this box to further tweak how the fingerprinting operation works. Consult with a Lynx
    expert if you think you need this.
  '''


def computeInputs(*args, **kwargs):
  '''  Triggers the computations for all entities associated with its input.

   - For table inputs, it computes the table.
   - For graph inputs, it computes the vertices and edges, all attributes,
     and the same transitively for all segments plus the segmentation links.


  '''


def computePagerank(*args, **kwargs):
  '''  Calculates http://en.wikipedia.org/wiki/PageRank[PageRank] for every vertex.
  PageRank is calculated by simulating random walks on the graph. Its PageRank
  reflects the likelihood that the walk leads to a specific vertex.

  Let's imagine a social graph with information flowing along the edges. In this case high
  PageRank means that the vertex is more likely to be the target of the information.

  Similarly, it may be useful to identify information sources in the reversed graph.
  Simply reverse the edges before running the operation to calculate the reverse PageRank.

  :param name:   The new attribute will be created under this name.
  :param weights:   The edge weights. Edges with greater weight correspond to higher probabilities
    in the theoretical random walk.
  :param iterations:   PageRank is an iterative algorithm. More iterations take more time but can lead
    to more precise results. As a rule of thumb set the number of iterations to the
    diameter of the graph, or to the median shortest path.
  :param damping:   The probability of continuing the random walk at each step. Higher damping
    factors lead to longer random walks.
  :param direction:   - `incoming edges`: Simulate random walk in the reverse edge direction.
       Finds the most influential sources.
     - `outgoing edges`: Simulate random walk in the original edge direction.
       Finds the most popular destinations.
     - `all edges`: Simulate random walk in both directions.
  '''


def mergeVerticesByAttribute(*args, **kwargs):
  '''  Merges each set of vertices that are equal by the chosen attribute. Vertices where the chosen
  attribute is not defined are discarded. Aggregations can be specified for how to handle the rest of
  the attributes, which may be different among the merged vertices. Any edge that connected two
  vertices that are merged will become a loop.

  Merge vertices by attributes might create parallel links between the base graph
  and its segmentations. If it is important that there are no such parallel links
  (e.g. when performing aggregations to and from segmentations),
  make sure to run the <<merge-parallel-segmentation-links, Merge parallel segmentation links>>
  operation on the segmentations in question.

  :param key:   If a set of vertices have the same value for the selected attribute, they will all be merged
    into a single vertex.

    include::{g}
  '''


def createRandomEdges(*args, **kwargs):
  '''  Creates edges randomly, so that each vertex will have a degree uniformly chosen between 0 and
  2 &times; the provided parameter.

  For example, you can create a random graph by first applying operation <<Create vertices>>
  and then creating the random edges.

  :param degree:   The degree of a vertex will be chosen uniformly between 0 and 2 &times; this number.
    This results in generating _number of vertices &times; average degree_ edges.
  :param seed:   The random seed.
    +
    include::{g}
  '''


def aggregateVertexAttributeGlobally(*args, **kwargs):
  '''  Aggregates vertex attributes across the entire graph into one graph attribute for each attribute.
  For example you could use it to calculate the average age across an entire dataset of people.

  :param prefix:   Save the aggregated values with this prefix.

    include::{g}
  '''


def sql10(*args, **kwargs):
  '''  Executes an SQL query on its ten inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`, `four`, `five`, `six`, `seven`,
  `eight`, `nine`, `ten`. For example:

  ```
  select * from one
  union select * from two
  union select * from three
  union select * from four
  union select * from five
  union select * from six
  union select * from seven
  union select * from eight
  union select * from nine
  union select * from ten
  ```

  :prefix: one.
  :maybe-tick: {backtick}
  include::{g}[tag


  '''


def useTableAsVertexAttributes(*args, **kwargs):
  '''  Imports vertex attributes for existing vertices from a table. This is
  useful when you already have vertices and just want to import one or more attributes.

  There are two different use cases for this operation:
  - Import using unique vertex attribute values. For example if the vertices represent people
  this attribute can be a personal ID. In this case the operation fails in case of duplicate
  attribute values (either among vertices or in the table).
  - Import using a normal vertex attribute. For example this can be a city of residence (vertices
  are people) and we can import census data for those cities for each person. Here the operation
  allows duplications of cities among vertices (but not in the lookup table).

  :param id_attr:   The String vertex attribute which is used to join with the table's ID column.
  :param id_column:   The ID column name in the table. This should be a String column that uses the values
    of the chosen vertex attribute as IDs.
  :param prefix:   Prepend this prefix string to the new vertex attribute names. This can be used to avoid
    accidentally overwriting existing attributes.
  :param unique_keys:   Assert that the vertex attribute values have to be unique if set true. The values of the
    matching ID column in the table have to be unique in both cases.
  '''


def predictVertexAttribute(*args, **kwargs):
  '''  If an attribute is defined for some vertices but not for others, machine learning can be used to
  fill in the blanks. A model is built from the vertices where the attribute is defined and the
  model predictions are generated for all the vertices.

  The prediction is created in a new attribute named after the predicted attribute, such as
  `age_prediction`.

  This operation only supports `number`-typed attributes. You can come up with ways to
  map other types to numbers to include them in the prediction. For example mapping gender to `0.0`
  and `1.0` makes sense.

  :param label:   The partially defined attribute that you want to predict.
  :param features:   The attributes that will be used as the input of the predictions. Predictions will be
    generated for vertices where all of the predictors are defined.
  :param method:   +
     - **Linear regression** with no regularization.
     - **Ridge regression** (also known as Tikhonov regularization) with L2-regularization.
     - **Lasso** with L1-regularization.
     - **Logistic regression** for binary classification. (The predicted attribute must be 0 or 1.)
     - **Naive Bayes** classifier with multinomial event model.
     - **Decision tree** with maximum depth 5 and 32 bins for all features.
     - **Random forest** of 20 trees of depth 5 with 32 bins. One third of features are considered
       for splits at each node.
     - **Gradient-boosted trees** produce ensembles of decision trees with depth 5 and 32 bins.
  '''


def importJson(*args, **kwargs):
  '''  JSON is a rich human-readable data format. JSON files are larger than CSV files but can represent
  data types. Each line of the file in this format stores one record encoded as a
  https://en.wikipedia.org/wiki/JSON[JSON] object.

  :param filename:   Upload a file by clicking the
    +++<label class
  '''


def externalComputation7(*args, **kwargs):
  '''  include::{g}[tag


  '''


def discardEdges(*args, **kwargs):
  '''  Throws away all edges. This implies discarding all edge attributes too.


  '''


def externalComputation6(*args, **kwargs):
  '''  include::{g}[tag


  '''


def setVertexAttributeIcons(*args, **kwargs):
  '''  Associates icons vertex attributes. It has no effect beyond highlighting something on the
  user interface.

  The icons are a subset of the Unicode characters in the "emoji" range, as provided by the
  https://www.google.com/get/noto/help/emoji/[Google Noto Font].

  :param title:   Leave empty to _remove_ the icon for the corresponding attribute
    or add one of the supported icon names, such as `snowman_without_snow`.
  '''


def exportToHive(*args, **kwargs):
  '''  Export a table directly to https://hive.apache.org/[Apache Hive].

  :param table:   The name of the database table to export to.
  :param mode:   Describes whether LynxKite should expect a table to already exist and how to handle this case.
    +
    **The table must not exist** means the table will be created and it is an error if it already
    exists.
    +
    **Drop the table if it already exists** means the table will be deleted and re-created if
    it already exists. Use this mode with great care. This method cannot be used if you specify
    any fields to partition by, the reason being that the underlying Spark library will delete
    all other partitions in the table in this case.

    +
    **Insert into an existing table** requires the
    table to already exist and it will add the exported data at the end of the existing table.
  :param partition_by:   The list of column names (if any) which you wish the table to be partitioned by. This cannot
    be used in conjunction with the "Drop the table if it already exists" mode.
  '''


def deriveColumn(*args, **kwargs):
  '''  Derives a new column on a table input via an SQL expression. Outputs a table.

  :param name:   The name of the new column.
  :param value:   The SQL expression to define the new column.
  '''


def trainALogisticRegressionModel(*args, **kwargs):
  '''  Trains a logistic regression model using the graph's vertex attributes. The
  algorithm converges when the maximum number of iterations is reached or no
  coefficient has changed in the last iteration. The threshold of the model is
  chosen to maximize the https://en.wikipedia.org/wiki/F1_score[F-score].

  https://en.wikipedia.org/wiki/Logistic_regression[Logistic regression] measures
  the relationship between the categorical dependent variable and one or more
  independent variables by estimating probabilities using a logistic function.

  The current implementation of logistic regression only supports binary classes.

  :param name:   The model will be stored as a graph attribute using this name.
  :param label:   The vertex attribute for which the model is trained to classify. The attribute should
    be binary label of either 0.0 or 1.0.
  :param features:   Attributes to be used as inputs for the training algorithm.
  :param max_iter:   The maximum number of iterations (>
  '''


def useTableAsEdges(*args, **kwargs):
  '''  Imports edges from a table. Your vertices must have an identifying attribute, by which
  the edges can be attached to them.

  :param attr:   The IDs that are used in the file when defining the edges.
  :param src:   The table column that specifies the source of the edge.
  :param dst:   The table column that specifies the destination of the edge.
  '''


def setGraphAttributeIcon(*args, **kwargs):
  '''  Associates an icon with a graph attribute. It has no effect beyond highlighting something on the user
  interface.

  The icons are a subset of the Unicode characters in the "emoji" range, as provided by the
  https://www.google.com/get/noto/help/emoji/[Google Noto Font].

  :param name:   The graph attribute to highlight.
  :param icon:   One of the supported icon names, such as `snowman_without_snow`. Leave empty to _remove_ the icon.
  '''


def compareSegmentationEdges(*args, **kwargs):
  '''  Compares the edge sets of two segmentations and computes _precision_ and _recall_.
  In order to make this work, the edges of the both segmentation graphs should be
  matchable against each other. Therefore, this operation only allows comparing
  segmentations which were created using the <<Use base graph as segmentation>> operation
  from the same graph. (More precisely, a one to one correspondence is needed between
  the vertices of both segmentations and the base graph.)

  You can use this operation for example to evaluate different colocation results against
  a reference result.

  :param golden:   Segmentation containing the golden edges.
  :param test:   Segmentation containing the test edges.
  '''


def mergeParallelEdgesByAttribute(*args, **kwargs):
  '''  Multiple edges going from A to B that share the same value of the given edge attribute
  will be merged into a single edge. The edges going from A to B are not merged with edges
  going from B to A.

  :param key:   The edge attribute on which the merging will be based.

    include::glossary.asciidoc
  '''


def copyVertexAttributesToSegmentation(*args, **kwargs):
  '''  Copies all vertex attributes from the parent to the segmentation.

  This operation available only when each segment contains just one vertex.

  :param prefix:   A prefix for the new attribute names. Leave empty for no prefix.
  '''


def fingerprintBasedOnAttributes(*args, **kwargs):
  '''  In a graph that has two different String identifier attributes (e.g. Facebook ID and
  MSISDN) this operation will match the vertices that only have the first attribute defined
  with the vertices that only have the second attribute defined. For the well-matched vertices
  the new attributes will be added. (For example if a vertex only had an MSISDN and we found a
  matching Facebook ID, this will be saved as the Facebook ID of the vertex.)

  The matched vertices will not be automatically merged, but this can easily be performed
  with the <<Merge vertices by attribute>> operation
  on either of the two identifier attributes.

  :param leftname:   Two identifying attributes have to be selected.
  :param rightname:   Two identifying attributes have to be selected.
  :param weights:   What `number` edge attribute to use as edge weight. The edge weights are also considered when
    calculating the similarity between two vertices.
  :param mo:   The number of common neighbors two vertices must have to be considered for matching.
    It must be at least 1. (If two vertices have no common neighbors their similarity would be zero
    anyway.)
  :param ms:   The similarity threshold below which two vertices will not be considered a match even if there are
    no better matches for them. Similarity is normalized to
  :param extra:   You can use this box to further tweak how the fingerprinting operation works. Consult with a Lynx
    expert if you think you need this.
  '''


def mapHyperbolicCoordinates(*args, **kwargs):
  '''  <<experimental-operation,+++<i class

  :param seed:   The random seed.
    +
    include::{g}
  '''


def discardSegmentation(*args, **kwargs):
  '''  Throws away a segmentation value.

  :param name:   The segmentation to discard.
  '''


def trainAKmeansClusteringModel(*args, **kwargs):
  '''  Trains a k-means clustering model using the graph's vertex attributes. The
  algorithm converges when the maximum number of iterations is reached or every
  cluster center does not move in the last iteration.

  https://en.wikipedia.org/wiki/K-means_clustering[k-means clustering] aims
  to partition _n_ observations into _k_ clusters in which each observation belongs
  to the cluster with the nearest mean, serving as a prototype of the cluster.

  For best results it may be necessary to scale the features before training the model.

  :param name:   The model will be stored as a graph attribute using this name.
  :param features:   Attributes to be used as inputs for the training algorithm. The trained model
    will have a list of features with the same names and semantics.
  :param k:   The number of clusters to be created.
  :param max_iter:   The maximum number of iterations (>
  :param seed:   The random seed.
  '''


def deriveVertexAttribute(*args, **kwargs):
  '''  Generates a new attribute based on existing vertex attributes. The value expression can be
  an arbitrary Scala expression, and it can refer to existing attributes as if they
  were local variables.

  For example you can write `age * 2` to generate a new attribute
  that is the double of the _age_ attribute. Or you can write
  `if (gender

  :param output:   The new attribute will be created under this name.
  :param defined_attrs:   - `true`: The new attribute will only be defined on vertices for which all the attributes used in the
      expression are defined.
    - `false`: The new attribute is defined on all vertices. In this case the Scala expression does not
      pass the attributes using their original types, but wraps them into `Option`s. E.g. if you have
      an attribute `income: Double` you would see it as `income: Option
  :param expr:   The Scala expression. You can enter multiple lines in the editor.
  :param persist:   If enabled, the output attribute will be saved to disk once it is calculated. If disabled, the
    attribute will be re-computed each time its output is used. Persistence can improve performance
    at the cost of disk space.
  '''


def copyGraphAttributeFromOtherGraph(*args, **kwargs):
  '''  This operation can take a graph attribute from another graph and copy it
  to the current graph.

  It can be useful if we trained a machine learning model in one graph, and would like
  to apply this model in another graph for predicting undefined attribute values.

  :param sourceproject:   The name of the other graph from where we want to copy a graph attribute.
  :param sourcescalarname:   The name of the graph attribute in the other graph. If it is a simple string, then
    the graph attribute with that name has to be in the root of the other graph. If it is
    a `.`-separated string, then it means a graph attribute in a segmentation of the other graph.
    The syntax for this case is: `seg_1.seg_2.....seg_n.graph_attribute`.
  :param destscalarname:   This will be the name of the copied graph attribute in this graph.
  '''


def weightedAggregateEdgeAttributeGlobally(*args, **kwargs):
  '''  Aggregates edge attributes across the entire graph into one graph attribute for each attribute.
  For example you could use it to calculate the total income as the sum of call durations
  weighted by the rates across an entire call dataset.

  :param prefix:   Save the aggregated values with this prefix.
  :param weight:   The `number` attribute to use as weight.

    include::{g}
  '''


def setEdgeAttributeIcons(*args, **kwargs):
  '''  Associates icons with edge attributes. It has no effect beyond highlighting something on the
  user interface.

  The icons are a subset of the Unicode characters in the "emoji" range, as provided by the
  https://www.google.com/get/noto/help/emoji/[Google Noto Font].

  :param title:   Leave empty to _remove_ the icon for the corresponding attribute
    or add one of the supported icon names, such as `snowman_without_snow`.
  '''


def findConnectedComponents(*args, **kwargs):
  '''  Creates a segment for every connected component of the graph.

  Connected components are maximal vertex sets where a path exists between each pair of vertices.

  :param name:   The new segmentation will be saved under this name.
  :param directions:   Ignore directions:::
    The algorithm adds reversed edges before calculating the components.
    Require both directions:::
    The algorithm discards non-symmetric edges before calculating the components.
  '''


def mergeParallelEdges(*args, **kwargs):
  '''  Multiple edges going from A to B will be merged into a single edge.
  The edges going from A to B are not merged with edges going from B to A.

  Edge attributes can be aggregated across the merged edges.


  '''


def takeSegmentationLinksAsBaseGraph(*args, **kwargs):
  '''  Replaces the current graph with the links from its base graph to the selected segmentation, represented
  as vertices. The vertices will have `base_` and `segment_` prefixed attributes generated for the
  attributes on the base graph and the segmentation respectively.


  '''


def deriveEdgeAttribute(*args, **kwargs):
  '''  Generates a new attribute based on existing attributes. The value expression can be
  an arbitrary Scala expression, and it can refer to existing attributes on the edge as if
  they were local variables. It can also refer to attributes of the source and destination
  vertex of the edge using the format `src$attribute` and `dst$attribute`.

  For example you can write `weight * scala.math.abs(src$age - dst$age)` to generate a new
  attribute that is the weighted age difference of the two endpoints of the edge.

  You can also refer to graph attributes in the Scala expression. For example,
  assuming that you have a graph attribute _age_average_, you can use the expression
  `if (src$age < age_average / 2 && dst$age > age_average * 2) 1.0 else 0.0`
  to identify connections between relatively young and relatively old people.

  Back quotes can be used to refer to attribute names that are not valid Scala identifiers.

  The Scala expression can return any of the following types:
  - `String`,
  - `Double`, which will be presented as `number`
  - `Int`, which will be automatically converted to `Double`
  - `Long`, which will be automatically converted to `Double`
  - `Vector`s or `Set`s combined from the above.

  In case you do not want to define the output for every input, you can return an `Option`
  created from the above types. E.g. `if (income > 1000) Some(age) else None`.

  :param output:   The new attribute will be created under this name.
  :param defined_attrs:   - `true`: The new attribute will only be defined on edges for which all the attributes used in the
      expression are defined.
    - `false`: The new attribute is defined on all edges. In this case the Scala expression does not
      pass the attributes using their original types, but wraps them into `Option`s. E.g. if you have
      an attribute `income: Double` you would see it as `income: Option
  :param expr:   The Scala expression. You can enter multiple lines in the editor.
  :param persist:   If enabled, the output attribute will be saved to disk once it is calculated. If disabled, the
    attribute will be re-computed each time its output is used. Persistence can improve performance
    at the cost of disk space.
  '''


def useTableAsEdgeAttributes(*args, **kwargs):
  '''  Imports edge attributes for existing edges from a table. This is
  useful when you already have edges and just want to import one or more attributes.

  There are two different use cases for this operation:
  - Import using unique edge attribute values. For example if the edges represent relationships
  between people (identified by `src` and `dst` IDs) we can import the number of total calls between
  each two people. In this case the operation fails for duplicate attribute values - i.e.
  parallel edges.
  - Import using a normal edge attribute. For example if each edge represents a call and the location
  of the person making the call is an edge attribute (cell tower ID) we can import latitudes and
  longitudes for those towers. Here the tower IDs still have to be unique in the lookup table.

  :param id_attr:   The edge attribute which is used to join with the table's ID column.
  :param id_column:   The ID column name in the table. This should be a String column that uses the values
    of the chosen edge attribute as IDs.
  :param prefix:   Prepend this prefix string to the new edge attribute names. This can be used to avoid
    accidentally overwriting existing attributes.
  :param unique_keys:   Assert that the edge attribute values have to be unique if set true. The values of the
    matching ID column in the table have to be unique in both cases.
  '''


def connectVerticesOnAttribute(*args, **kwargs):
  '''  Creates edges between vertices that are equal in a chosen attribute. If the source attribute of A
  equals the destination attribute of B, an A{to}B edge will be generated.

  The two attributes must be of the same data type.

  For example, if you connect nodes based on the "name" attribute, then everyone called "John
  Smith" will be connected to all the other "John Smiths".

  :param fromattr:   An A{to}B edge is generated when this attribute on A matches the destination attribute on B.
  :param toattr:   An A{to}B edge is generated when the source attribute on A matches this attribute on B.
  '''


def importOrc(*args, **kwargs):
  '''  https://orc.apache.org/[Apache ORC] is a columnar data storage format.

  :param filename:   The distributed file-system path of the file. See <<prefixed-paths>> for more details on specifying
    paths.

    include::{g}
  '''


def externalComputation1(*args, **kwargs):
  '''  include::{g}[tag


  '''


def findSteinerTree(*args, **kwargs):
  '''  Given a directed graph in which each vertex has two associated quantities, the "gain",
  and the "root cost", and each edge has an associated quantity, the "cost",
  this operation will yield a forest (a set of trees) that is a subgraph of the given
  graph. Furthermore, in this subgraph, the sum of the gains
  minus the sum of the (edge and root) costs approximate the maximal possible value.

  Finding this optimal subgraph is called the
  https://en.wikipedia.org/wiki/Steiner_tree_problem#Steiner_tree_in_graphs_and_variants[Prize-collecting Steiner Tree Problem].

  The operation will result in four outputs: (1) A new edge attribute, which will specify which
  edges are part of the optimal solution. Its value will be 1.0 for edges that
  are part of the optimal forest and not defined otherwise; (2) A new vertex
  attribute, which will specify which vertices are part of the optimal solution.
  Its value will be 1.0 for vertices that are part of the optimal forest and not defined otherwise.
  (3) A new graph attribute that contains the net gain, that is, the total sum of the gains
  minus the total sum of the (edge and root) costs; and
  (4) A new vertex attribute that will specify the root vertices in the
  optimal solution: it will be 1.0 for the root vertices and not defined otherwise.

  :param ename:   The new edge attribute will be created under this name, to pinpoint the edges
    in the solution.
  :param vname:   The new vertex attribute will be created under this name, to pinpoint the vertices
    in the solution.
  :param pname:   The profit will be reported under this name.
  :param rname:   The new vertex attribute will be created under this name, to pinpoint the tree
    roots in the optimal solution.
  :param edge_costs:   This edge attribute specified here will determine the cost for including the
    given edge in the solution. Negative and undefined values are treated as 0.
  :param root_costs:   The vertex attribute specified here determines the cost for allowing
    the given vertex to be a starting point (the root) of a tree in the solution forest.
    Negative or undefined values mean that the vertex cannot be used as a root point.
  :param gain:   This vertex attribute specifies the reward (gain) for including the given
    vertex in the solution. Negative or undefined values are treated as 0.
  '''


def fillVertexAttributesWithConstantDefaultValues(*args, **kwargs):
  '''  An attribute may not be defined on every vertex. This operation sets a default value
  for the vertices where it was not defined.

  :param title:   The given value will be set for vertices where the attribute is not defined. No change for
    attributes for which the default value is left empty. The default value
    must be numeric for `number` attributes.
  '''


def trainADecisionTreeClassificationModel(*args, **kwargs):
  '''  Trains a decision tree classifier model using the graph's vertex attributes.
  The algorithm recursively partitions the feature space into two parts. The tree
  predicts the same label for each bottommost (leaf) partition. Each binary
  partitioning is chosen from a set of possible splits in order to maximize the
  information gain at the corresponding tree node. For calculating the information
  gain the impurity of the nodes is used (read more about impurity at the description
  of the impurity parameter): the information gain is the difference between the
  parent node impurity and the weighted sum of the two child node impurities.
  https://spark.apache.org/docs/latest/mllib-decision-tree.html#basic-algorithm[More information about the parameters.]

  :param name:   The model will be stored as a graph attribute using this name.
  :param label:   The vertex attribute the model is trained to predict.
  :param features:   The attributes the model learns to use for making predictions.
  :param impurity:   Node impurity is a measure of homogeneity of the labels at the node and is used
    for calculating the information gain. There are two impurity measures provided.
    +
      - **Gini:** Let _S_ denote the set of training examples in this node. Gini
      impurity is the probability of a randomly chosen element of _S_ to get an incorrect
      label, if it was randomly labeled according to the distribution of labels in _S_.
      - **Entropy:** Let _S_ denote the set of training examples in this node, and
      let _f~i~_ be the ratio of the _i_ th label in _S_. The entropy of the node is
      the sum of the _-p~i~log(p~i~)_ values.
  :param maxbins:   Number of bins used when discretizing continuous features.
  :param maxdepth:   Maximum depth of the tree.
  :param mininfogain:   Minimum information gain for a split to be considered as a tree node.
  :param minInstancesPerNode:   For a node to be split further, the split must improve at least this much
    (in terms of information gain).
  :param seed:   We maximize the information gain only among a subset of the possible splits.
    This random seed is used for selecting the set of splits we consider at a node.
  '''


def sql8(*args, **kwargs):
  '''  Executes an SQL query on its eight inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`, `four`, `five`, `six`, `seven`,
  `eight`. For example:

  ```
  select * from one
  union select * from two
  union select * from three
  union select * from four
  union select * from five
  union select * from six
  union select * from seven
  union select * from eight
  ```

  :prefix: one.
  :maybe-tick: {backtick}
  include::{g}[tag


  '''


def sql9(*args, **kwargs):
  '''  Executes an SQL query on its nine inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`, `four`, `five`, `six`, `seven`,
  `eight`, `nine`. For example:

  ```
  select * from one
  union select * from two
  union select * from three
  union select * from four
  union select * from five
  union select * from six
  union select * from seven
  union select * from eight
  union select * from nine
  ```

  :prefix: one.
  :maybe-tick: {backtick}
  include::{g}[tag


  '''


def defineSegmentationLinksFromMatchingAttributes(*args, **kwargs):
  '''  Connect vertices in the base graph with segments based on matching attributes.

  This operation can be used (among other things) to create connections between two graphs once
  one has been imported as a segmentation of the other.
  (See <<Use other graph as segmentation>>.)

  :param base_id_attr:   A vertex will be connected to a segment if the selected vertex attribute of the vertex
    matches the selected vertex attribute of the segment.
  :param seg_id_attr:   A vertex will be connected to a segment if the selected vertex attribute of the vertex
    matches the selected vertex attribute of the segment.
  '''


def computeDispersion(*args, **kwargs):
  '''  Calculates the extent to which two people's mutual friends are not themselves well-connected.
  The dispersion attribute for an A{to}B edge is the number of pairs of nodes that are both
  connected to A and B but are not directly connected to each other.

  Dispersion ignores edge directions.

  It is a useful signal for identifying romantic partnerships -- connections with high dispersion --
  according to http://arxiv.org/abs/1310.6753[
    _Romantic Partnerships and the Dispersion of Social Ties:
    A Network Analysis of Relationship Status on Facebook_].

  A normalized dispersion metric is also generated by this operation. This is normalized against the
  embeddedness of the edge with the formula recommended in the cited article.
  (_disp(u,v)^0.61^/(emb(u,v)+5)_) It does not necessarily fall in the _(0,1)_ range.

  :param name:   The new edge attribute will be created under this name.
  '''


def input(*args, **kwargs):
  '''  This special box represents an input that comes from outside of this workspace.
  This box will not have a valid output on its own. When this workspace is used as a custom
  box in another workspace, the custom box will have one input for each input box.
  When the inputs are connected, those input states will appear on the outputs of the input boxes.

  Input boxes without a name are ignored. Each input box must have a different name.

  See the section on <<custom-boxes>> on how to use this box.

  :param name:   The name of the input, when the workspace is used as a custom box.
  '''


def useTableAsSegmentationLinks(*args, **kwargs):
  '''  Import the connection between the main graph and this segmentation from a table.
  Each row in the table represents a connection between one base vertex and one segment.

  :param base_id_attr:   The `String` vertex attribute that can be joined to the identifying column in the table.
  :param base_id_column:   The table column that can be joined to the identifying attribute on the base graph.
  :param seg_id_attr:   The `String` vertex attribute that can be joined to the identifying column in the table.
  :param seg_id_column:   The table column that can be joined to the identifying attribute on the segmentation.
  '''


def useOtherGraphAsSegmentation(*args, **kwargs):
  '''  Copies another graph into a new segmentation for this one. There will be no
  connections between the segments and the base vertices. You can import/create those via
  other operations. (See <<Use table as segmentation links>> and
  <<Define segmentation links from matching attributes>>.)

  It is possible to import the graph itself as segmentation. But even in this
  special case, there will be no connections between the segments and the base vertices.
  Another operation, <<Use base graph as segmentation>> can be used if edges are desired.


  '''


def splitToTrainAndTestSet(*args, **kwargs):
  '''  Based on the source attribute, 2 new attributes are created, source_train and source_test.
  The attribute is partitioned, so every instance is copied to either the training or the test set.

  :param source:   The attribute you want to create train and test sets from.
  :param test_set_ratio:   A test set is a random sample of the vertices. This parameter gives the size of the test set
    as a fraction of the total vertex count.
  :param seed:   Random seed.
    +
    include::{g}
  '''


def segmentByGeographicalProximity(*args, **kwargs):
  '''  Creates a segmentation from the features in a Shapefile. A vertex is connected to a segment if the
  the `position` vertex attribute is within a specified distance from the segment's geometry
  attribute. Feature attributes from the Shapefile become segmentation attributes.

  * The lookup depends on the coordinate reference system and distance metric of the feature. All
    inputs must use the same coordinate reference system and distance metric.
  * This algorithm creates an overlapping segmentation since one vertex can be sufficiently close to
    multiple GEO segments.

  Shapefiles can be obtained from various sources, like
  http://wiki.openstreetmap.org/wiki/Shapefiles[OpenStreetMap].

  :param name:   The name of the new geographical segmentation.
  :param position:   The (latitude, longitude) location tuple.
  :param shapefile:   The https://en.wikipedia.org/wiki/Shapefile
  :param distance:   Vertices are connected to geographical segments if within this distance. The distance has to use
    the same metric and coordinate reference system as the features within the Shapefile.
  :param ignoreUnsupportedShapes:   If set `true`, silently ignores unknown shape types potentially contained by the Shapefile.
    Otherwise throws an error.
  '''


def graphUnion(*args, **kwargs):
  '''  The resulting graph is just a disconnected graph containing the vertices and edges of
  the two originating graphs. All vertex and edge attributes are preserved. If an attribute
  exists in both graphs, it must have the same data type in both.

  The resulting graph will have as many vertices as the sum of the vertex counts in the two
  source graphs. The same with the edges.

  Segmentations are discarded.


  '''


def findTriangles(*args, **kwargs):
  '''  Creates a segment for every triangle in the graph.
  A triangle is defined as 3 pairwise connected vertices, regardless of the direction and number of edges between them.
  This means that triangles with one or more multiple edges are still only counted once,
  and the operation does not differentiate between directed and undirected triangles.
  Since one vertex can be part of multiple triangles this segmentation might be overlapping.

  :param name:   The new segmentation will be saved under this name.
  :param bothdir:   Whether edges have to exist in both directions between all members of a triangle.
    +
    If the direction of the edges is not important, set this to `false`. This will allow placing two
    vertices into the same clique even if they are only connected in one direction.
  '''


def segmentByEventSequence(*args, **kwargs):
  '''  Treat vertices as people attending events, and segment them by attendance of sequences of events.
  There are several algorithms for generating event sequences, see under
  <<segment-by-event-sequence-algorithm, Algorithm>>.

  This operation runs on a segmentation which contains events as vertices, and it is a segmentation
  over a graph containing people as vertices.

  :param name:   The new segmentation will be saved under this name.
  :param time_attr:   The `number` attribute corresponding the time of events.
  :param location:   A segmentation over events or an attribute corresponding to the location of events.
  :param algorithm:   * *Take continuous event sequences*:
    Merges subsequent events of the same location, and then takes all the continuous event sequences
    of length _Time window length_, with maximal timespan of _Time window length_. For each of these
    events, a segment is created for each time bucket the starting event falls into. Time buckets
    are defined by _Time window step_ and bucketing starts from 0.0 time.

    * *Allow gaps in event sequences:*
    Takes all event sequences that are no longer than _Time window length_ and then creates a segment
    for each subsequence with _Sequence length_.
  :param sequence_length:   Number of events in each segment.
  :param time_window_step:   Bucket size used for discretizing events.
  :param time_window_length:   Maximum time difference between first and last event in a segment.
  '''


def convertVertexAttributeToString(*args, **kwargs):
  '''  Converts the selected vertex attributes to `String` type.

  The attributes will be converted in-place. If you want to keep the original attributes as
  well, make a copy first!

  :param attr:   The attributes to be converted.
  '''


def trainADecisionTreeRegressionModel(*args, **kwargs):
  '''  Trains a decision tree regression model using the graph's vertex attributes.
  The algorithm recursively partitions the feature space into two parts. The tree
  predicts the same label for each bottommost (leaf) partition. Each binary
  partitioning is chosen from a set of possible splits in order to maximize the
  information gain at the corresponding tree node. For calculating the information
  gain the variance of the nodes is used:
  the information gain is the difference between the parent node variance and the
  weighted sum of the two child node variances.
  https://spark.apache.org/docs/latest/mllib-decision-tree.html#basic-algorithm[More information about the parameters.]

  Note: Once the tree is trained there is only a finite number of possible predictions.
  Because of this, the regression model might seem like a classification. The main
  difference is that these buckets ("classes") are invented by the algorithm during
  the training in order to minimize the variance.

  :param name:   The model will be stored as a graph attribute using this name.
  :param label:   The vertex attribute the model is trained to predict.
  :param features:   The attributes the model learns to use for making predictions.
  :param maxbins:   Number of bins used when discretizing continuous features.
  :param maxdepth:   Maximum depth of the tree.
  :param mininfogain:   Minimum information gain for a split to be considered as a tree node.
  :param minInstancesPerNode:   For a node to be split further, the split must improve at least this much
    (in terms of information gain).
  :param seed:   We maximize the information gain only among a subset of the possible splits.
    This random seed is used for selecting the set of splits we consider at a node.
  '''


def copyGraphAttribute(*args, **kwargs):
  '''  Creates a copy of a graph attribute.

  :param from:
  :param to:
  '''


def weightedAggregateToSegmentation(*args, **kwargs):
  '''  Aggregates vertex attributes across all the vertices that belong to a segment.
  For example, it can calculate the average age per kilogram of each clique.

  :param weight:   The `number` attribute to use as weight.

    include::{g}
  '''


def customPlot(*args, **kwargs):
  '''  Creates a plot from the input table. The plot can be defined using the
  https://github.com/vegas-viz/Vegas[Vegas] plotting API in Scala. This API makes
  it easy to define https://vega.github.io/vega-lite/[Vega-Lite] plots in code.

  You code has to evaluate to a `vegas.Vegas` object. For your convenience `vegas._` is already
  imported. An example of a simple plot would be:

  ```
  Vegas()
    .withData(table)
    .encodeX("name", Nom)
    .encodeY("age", Quant)
    .encodeColor("gender", Nom)
    .mark(Bar)
  ```

  `Vegas()` is the entry point to the plotting API. You can provide a title if you like: `Vegas("My
  Favorite Plot")`.

  LynxKite fetches a sample of up to 10,000 rows from your table for the purpose of the plot. This
  data is made available in the `table` variable (as `Seq[Map[String, Any]]`). `.withData(table)`
  binds this data to the plot. You can transform the data before plotting if necessary:

  ```
  val doubled

  :param plot_code:   Scala code for defining the plot.
  '''


def useTableAsVertices(*args, **kwargs):
  '''  Imports vertices (no edges) from a table.
  Each column in the table will be accessible as a vertex attribute.


  '''


def sql2(*args, **kwargs):
  '''  Executes an SQL query on its two inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one` and `two`. For example:

  ```
  select one.*, two.*
  from one
  join two
  on one.id


  '''


def sql3(*args, **kwargs):
  '''  Executes an SQL query on its three inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`. For example:

  ```
  select one.*, two.*, three.*
  from one
  join two
  join three
  on one.id


  '''


def convertVertexAttributeToDouble(*args, **kwargs):
  '''  Converts the selected `String` typed vertex attributes to the `number` type.

  The attributes will be converted in-place. If you want to keep the original `String` attribute as
  well, make a copy first!

  :param attr:   The attributes to be converted.
  '''


def coloring(*args, **kwargs):
  '''  Finds a coloring of the vertices of the graph with no two neighbors with the same color. The colors are represented by
  numbers. Tries to find a coloring with few colors.

  Vertex coloring is used in scheduling problems to distribute resources among parties which simultaneously
  and asynchronously request them.
  https://en.wikipedia.org/wiki/Graph_coloring

  :param name:   The new attribute will be created under this name.
  '''


def trainAGcnRegressor(*args, **kwargs):
  '''  Trains a https://tkipf.github.io/graph-convolutional-networks/[Graph Convolutional Network]
  using https://pytorch-geometric.readthedocs.io/en/latest/[Pytorch Geometric].
  Applicable for regression problems.

  :param save_as:   The resulting model will be saved as a graph attribute using this name.
  :param iterations:   Number of training iterations.
  :param features:   Vector attribute containing the features to be used as inputs for the training algorithm.
  :param label:   The attribute we want to predict.
  :param forget:   Set true to allow a vertex to see the labels of its neighbors and use them for
    predicting its own label.
  :param batch_size:   In each iteration of the training, we compute the error only on a subset of the vertices.
    Batch size specifies the size of this subset.
  :param learning_rate:   Value of the learning rate.
  :param hidden_size:   Size of the hidden layers.
  :param num_conv_layers:   Number of convolution layers.
  :param conv_op:   The type of graph convolution to use.
    https://pytorch-geometric.readthedocs.io/en/latest/modules/nn.html#torch_geometric.nn.conv.GCNConv
  :param seed:   Random seed for initializing network weights and choosing training batches.
  '''


def sampleGraphByRandomWalks(*args, **kwargs):
  '''  This operation realizes a random walk on the graph which can be used as a small smart sample to
  test your model on. The walk starts from a randomly selected vertex and at every step either aborts
  the current walk (with probability _Walk abortion probability_) and jumps back to the start point
  or moves to a randomly selected (directed sense) neighbor of the current vertex. After _Number of
  walks from each start point_ restarts it selects a new start vertex. After _Number of start points_
  new start points were selected, it stops. The performance of this algorithm according to different
  metrics can be found in the following publication,
  https://cs.stanford.edu/people/jure/pubs/sampling-kdd06.pdf.

  The output of the operation is a vertex and an edge attribute which describes which was the first
  step that ended at the given vertex / traversed the given edge. The attributes are not defined on
  vertices that were never reached or edges that were never traversed.

  If the resulting sample is still too large, it can be quickly reduced by keeping only the low index
  nodes and edges. Obtaining a sample with exactly `n` vertices is also possible with the
  following procedure.

  . Run this operation. Let us denote the computed vertex attribute by `first_reached` and edge
  attribute by `first_traversed`.
  . Rank the vertices by `first_reached`.
  . Filter the vertices by the rank attribute to keep the only vertex of rank `n`.
  . Aggregate `first_reached` to a graph attribute on the filtered graph (use either _average_, _first_,
  _max_, _min_, or _most_common_ - there is only one vertex in the filtered graph).
  . Filter the vertices and edges of the original graph and keep the ones that have smaller or equal
  `first_reached` or `first_traversed` values than the value of the derived graph attribute.

  :param startpoints:   The number of times a new start point is selected.
  :param walksfromonepoint:   The number of times the random walk restarts from the same start point before selecting a new start
    point.
  :param walkabortionprobability:   The probability of aborting a walk instead of moving along an edge. Therefore the length of the
    parts of the walk between two abortions follows a geometric distribution with parameter _Walk
    abortion probability_.
  :param vertexattrname:   The name of the attribute which shows which step reached the given vertex first. It is not defined
    on vertices that were never reached.
  :param edgeattrname:   The name of the attribute which shows which step traversed the given edge first. It is not defined
    on edges that were never traversed.
  :param seed:   The random seed.
    +
    include::{g}
  '''


def renameGraphAttributes(*args, **kwargs):
  '''  Changes the name of graph attributes.

  :param title:   If the new name is empty, the attribute will be discarded.
  '''


def addConstantEdgeAttribute(*args, **kwargs):
  '''  Adds an attribute with a fixed value to every edge.

  :param name:   The new attribute will be created under this name.
  :param value:   The attribute value. Should be a number if _Type_ is set to `number`.
  :param type:   The operation can create either `number` (numeric) or `String` typed attributes.
  '''


def computeCentrality(*args, **kwargs):
  '''  Calculates an approximation of the centrality for every vertex. Higher centrality means that
  the vertex is more embedded in the graph. Multiple different centrality measures have been defined
  in the literature. You can choose the specific centrality measure as a parameter to this operation.

  :param name:   The new attribute will be created under this name.
  :param maxdiameter:   The algorithm works by counting the shortest paths up to a certain length in each iteration.
    This parameter sets the maximal length to check, so it has a strong influence over the run
    time of the operation.
    +
    A setting lower than the actual diameter of the graph can theoretically introduce unbounded error
    to the results. In typical small world graphs this effect may be acceptable, however.
  :param algorithm:   - The https://en.wikipedia.org/wiki/Centrality#Harmonic_centrality
  :param bits:   The centrality algorithm is an approximation. This parameter sets the trade-off between
    the quality of the approximation and the memory and time consumption of the algorithm.
    In most cases the default value is good enough. On very large graphs it may help to use
    a lower number in order to speed up the algorithm or meet memory constraints.
  :param direction:   - `incoming edges`: Calculating paths _from_ vertices.
     - `outgoing edges`: Calculating paths _to_ vertices.
     - `all edges`: Calculating paths to both directions - effectively on an undirected graph.
  '''


def correlateTwoAttributes(*args, **kwargs):
  '''  Calculates the Pearson correlation coefficient of two attributes.
  Only vertices where both attributes are defined are considered.

  Note that correlation is undefined if at least one of the
  attributes is a constant.

  :param attra:   The correlation of these two attributes will be calculated.
  :param attrb:   The correlation of these two attributes will be calculated.
  '''


def setSegmentationIcon(*args, **kwargs):
  '''  Associates an icon with a segmentation. It has no effect beyond highlighting something on the user
  interface.

  The icons are a subset of the Unicode characters in the "emoji" range, as provided by the
  https://www.google.com/get/noto/help/emoji/[Google Noto Font].

  This operation is more easily accessed from the segmentation's dropdown menu in the graph state view.

  :param name:   The segmentation to highlight.
  :param icon:   One of the supported icon names, such as `snowman_without_snow`. Leave empty to _remove_ the icon.
  '''


def transform(*args, **kwargs):
  '''  Transforms all columns of a table input via SQL expressions. Outputs a table.

  An input parameter is generated for every table column. The parameters are
  SQL expressions interpreted on the input table. The default value leaves the column alone.


  '''


def aggregateEdgeAttributeToVertices(*args, **kwargs):
  '''  Aggregates an attribute on all the edges going in or out of vertices.
  For example it can calculate the average duration of calls for each person in a call dataset.

  :param prefix:   Save the aggregated attributes with this prefix.
  :param direction:   - `incoming edges`: Aggregate across the edges coming in to each vertex.
     - `outgoing edges`: Aggregate across the edges going out of each vertex.
     - `all edges`: Aggregate across all the edges going in or out of each vertex.

    include::{g}
  '''


def addReversedEdges(*args, **kwargs):
  '''  For every A{to}B edge adds a new B{to}A edge, copying over the attributes of the original.
  Thus this operation will double the number of edges in the graph.

  Using this operation you end up with a graph with symmetric edges: if A{to}B exists then
  B{to}A also exists. This is the closest you can get to an "undirected" graph.

  Optionally, a new edge attribute (a 'distinguishing attribute') will be created so that you can
  tell the original edges from the new edges after the operation. Edges where this attribute is 0
  are original edges; edges where this attribute is 1 are new edges.

  :param distattr:   The name of the distinguishing edge attribute; leave it empty if the attribute should not be created.
  '''


def createExampleGraph(*args, **kwargs):
  '''  Creates small test graph with 4 people and 4 edges between them.


  '''


def computeHyperbolicEdgeProbability(*args, **kwargs):
  '''  Adds edge attribute _hyperbolic edge probability_ based on
  hyperbolic distances between vertices. This indicates
  how likely that edge would be to exist if the input graph was
  probability x similarity-grown.
  On a general level it is a metric of *edge strength*.
  Probabilities are guaranteed to be 0

  :param radial:   The vertex attribute to be used as radial coordinates.
    Should not contain negative values.
  :param angular:   The vertex attribute to be used as angular coordinates.
    Values should be 0 - 2 * Pi.
  '''


def externalComputation4(*args, **kwargs):
  '''  include::{g}[tag


  '''


def externalComputation5(*args, **kwargs):
  '''  include::{g}[tag


  '''


def importCsv(*args, **kwargs):
  '''  CSV stands for comma-separated values. It is a common human-readable file format where each record
  is on a separate line and fields of the record are simply separated with a comma or other delimiter.
  CSV does not store data types, so all fields become strings when importing from this format.

  :param filename:   Upload a file by clicking the
    +++<label class
  :param columns:   The names of all the columns in the file, as a comma-separated list. If empty, the column names will
    be read from the file. (Use this if the file has a header.)
  :param delimiter:   The delimiter separating the fields in each line.
  :param quote:   The character used for escaping quoted values where the delimiter can be part of the value.
  :param escape:   The character used for escaping quotes inside an already quoted value.
  :param null_value:   The string representation of a `null` value in the CSV file. For example if set to `undefined`,
    every `undefined` value in the CSV file will be converted to Scala `null`-s.
    By default this is an empty string, so empty strings are converted to `null`-s upon import.
  :param date_format:   The string that indicates a date format. Custom date formats follow the formats at
    https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  :param timestamp_format:   The string that indicates a timestamp format. Custom date formats follow the formats at
    https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  :param ignore_leading_white_space:   A flag indicating whether or not leading whitespaces from values being read should be skipped.
  :param ignore_trailing_white_space:   A flag indicating whether or not trailing whitespaces from values being read should be skipped.
  :param comment:   Every line beginning with this character is skipped, if set. For example if the comment character is
    `#` the following line is ignored in the CSV file: `# This is a comment.`
  :param error_handling:   What should happen if a line has more or less fields than the number of columns?
    +
    **Fail on any malformed line** will cause the import to fail if there is such a line.
    +
    **Ignore malformed lines** will simply omit such lines from the table. In this case an erroneously
    defined column list can result in an empty table.
    +
    **Salvage malformed lines: truncate or fill with nulls** will still import the problematic lines,
    dropping some data or inserting undefined values.
  :param infer:   Automatically detects data types in the CSV. For example a column full of numbers will become a
    `Double`. If disabled, all columns are imported as ``String``s.

    include::{g}
  '''


def createEdgesFromCoOccurrence(*args, **kwargs):
  '''  Connects vertices in the base graph if they co-occur in any segments.
  Multiple co-occurrences will result in multiple parallel edges. Loop edges
  are generated for each segment that a vertex belongs to. The attributes of
  the segment are copied to the edges created from it.

  This operation has a potential to create trillions of edges or more.
  The number of edges created is the sum of squares of the segment sizes.
  It is recommended to drop very large segments before running this computation.


  '''


def oneHotEncodeAttribute(*args, **kwargs):
  '''  Encodes a categorical `String` attribute into a https://en.wikipedia.org/wiki/One-hot[one-hot]
  `Vector[number]`. For example, if you apply it to the `name` attribute of the example graph
  with categories `Adam,Eve,Isolated Joe,Sue`, you end up with

  |

  :param output:   The new attribute will be created under this name.
  :param catAttr:   The attribute you would like to turn into a one-hot Vector.
  :param categories:   Possible categories separated by commas.
  '''


def snowballSample(*args, **kwargs):
  '''  This operation creates a small smart sample of a graph. First, a subset of the original vertices is chosen
  for start points; the ratio of the size of this subset to the size of the original vertex set
  is the first parameter for the operation.
  Then a certain neighborhood of each start point is added to the sample; the radius of this neighborhood
  is controlled by another parameter.
  The result of the operation is a subgraph of the original graph consisting of the vertices of the sample and
  the edges between them.
  This operation also creates a new attribute which shows how far the sample vertices are from the
  closest start point. (One vertex can be in more than one neighborhood.)
  This attribute can be used to decide whether a sample vertex is near to a start point or not.

  For example, you can create a random sample of the graph to test your model on smaller data set.

  :param ratio:   The (approximate) fraction of vertices to use as starting points.
  :param radius:   Limits the size of the neighborhoods of the start points.
  :param attrname:   The name of the attribute which shows how far the sample vertices are from the closest start point.
  :param seed:   The random seed.
    +
    include::{g}
  '''


def weightedAggregateOnNeighbors(*args, **kwargs):
  '''  Aggregates across the vertices that are connected to each vertex. You can use
  the `Aggregate on` parameter to define how exactly this aggregation will take
  place: choosing one of the 'edges' settings can result in a neighboring
  vertex being taken into account several times (depending on the number of edges between
  the vertex and its neighboring vertex); whereas choosing one of the 'neighbors' settings
  will result in each neighboring vertex being taken into account once.

  For example, it can calculate the average age per kilogram of the friends of each person.

  :param prefix:   Save the aggregated attributes with this prefix.
  :param weight:   The `number` attribute to use as weight.
  :param direction:   - `incoming edges`: Aggregate across the edges coming in to each vertex.
     - `outgoing edges`: Aggregate across the edges going out of each vertex.
     - `all edges`: Aggregate across all the edges going in or out of each vertex.
     - `symmetric edges`:
       Aggregate across the 'symmetric' edges for each vertex: this means that if you have n edges
       going from A to B and k edges going from B to A, then min(n,k) edges will be
       taken into account for both A and B.
     - `in-neighbors`: For each vertex A, aggregate across those vertices
       that have an outgoing edge to A.
     - `out-neighbors`: For each vertex A, aggregate across those vertices
       that have an incoming edge from A.
     - `all neighbors`: For each vertex A, aggregate across those vertices
       that either have an outgoing edge to or an incoming edge from A.
     - `symmetric neighbors`: For each vertex A, aggregate across those vertices
       that have both an outgoing edge to and an incoming edge from A.

    include::{g}
  '''


def sampleEdgesFromCoOccurrence(*args, **kwargs):
  '''  Connects vertices in the parent graph with a given probability
  if they co-occur in any segments.
  Multiple co-occurrences will have the same chance of being selected
  as single ones. Loop edges are also included with the same probability.

  :param probability:   The probability of choosing a vertex pair. The expected value of the number of
    created vertices will be _probability * number of edges without parallel edges_.
  :param seed:   The random seed.
    +
    include::{g}
  '''


def output(*args, **kwargs):
  '''  This special box represents an output that goes outside of this workspace.
  When this workspace is used as a custom box in another workspace, the custom box
  will have one output for each output box.

  Output boxes without a name are ignored. Each output box must have a different name.

  See the section on <<custom-boxes>> on how to use this box.

  :param name:   The name of the output, when the workspace is used as a custom box.
  '''


def findModularClustering(*args, **kwargs):
  '''  Tries to find a partitioning of the vertices with high
  http://en.wikipedia.org/wiki/Modularity_(networks)[modularity].

  Edges that go between vertices in the same segment increase modularity, while edges that go from
  one segment to the other decrease modularity. The algorithm iteratively merges and splits segments
  and moves vertices between segments until it cannot find changes that would significantly improve
  the modularity score.

  :param name:   The new segmentation will be saved under this name.
  :param weights:   The attribute to use as edge weights.
  :param max_iterations:   After this number of iterations we stop regardless of modularity increment. Use -1 for unlimited.
  :param min_increment_per_iteration:   If the average modularity increment in the last few iterations goes below this then we stop
    the algorithm and settle with the clustering found.
  '''


def mergeTwoVertexAttributes(*args, **kwargs):
  '''  An attribute may not be defined on every vertex. This operation uses the secondary
  attribute to fill in the values where the primary attribute is undefined. If both are
  undefined on a vertex then the result is undefined too.

  :param name:   The new attribute will be created under this name.
  :param attr1:   If this attribute is defined on a vertex, then its value will be copied to the output attribute.
  :param attr2:   If the primary attribute is not defined on a vertex but the secondary attribute is, then the
    secondary attribute's value will be copied to the output variable.
  '''


def aggregateOnNeighbors(*args, **kwargs):
  '''  Aggregates across the vertices that are connected to each vertex. You can use
  the `Aggregate on` parameter to define how exactly this aggregation will take
  place: choosing one of the 'edges' settings can result in a neighboring
  vertex being taken into account several times (depending on the number of edges between
  the vertex and its neighboring vertex); whereas choosing one of the 'neighbors' settings
  will result in each neighboring vertex being taken into account once.

  For example, it can calculate the average age of the friends of each person.

  :param prefix:   Save the aggregated attributes with this prefix.
  :param direction:   - `incoming edges`: Aggregate across the edges coming in to each vertex.
     - `outgoing edges`: Aggregate across the edges going out of each vertex.
     - `all edges`: Aggregate across all the edges going in or out of each vertex.
     - `symmetric edges`:
       Aggregate across the 'symmetric' edges for each vertex: this means that if you have n edges
       going from A to B and k edges going from B to A, then min(n,k) edges will be
       taken into account for both A and B.
     - `in-neighbors`: For each vertex A, aggregate across those vertices
       that have an outgoing edge to A.
     - `out-neighbors`: For each vertex A, aggregate across those vertices
       that have an incoming edge from A.
     - `all neighbors`: For each vertex A, aggregate across those vertices
       that either have an outgoing edge to or an incoming edge from A.
     - `symmetric neighbors`: For each vertex A, aggregate across those vertices
       that have both an outgoing edge to and an incoming edge from A.

    include::{g}
  '''


def approximateEmbeddedness(*args, **kwargs):
  '''  Scalable algorithm to calculate the approximate overlap size of vertex neighborhoods
  along the edges. If an A{to}B edge has an embeddedness of `N`, it means A and B have
  `N` common neighbors. The approximate embeddedness is undefined for loop edges.

  :param name:   The new attribute will be created under this name.
  :param bits:   This algorithm is an approximation. This parameter sets the trade-off between
    the quality of the approximation and the memory and time consumption of the algorithm.
  '''


def classifyWithModel(*args, **kwargs):
  '''  Creates classifications from a model and vertex attributes of the graph. For the classifications
  with nominal outputs, an additional probability is created to represent the corresponding
  outcome probability.

  :param name:   The new attribute of the classification will be created under this name.
  :param model:   The model used for the classifications and a mapping from vertex attributes to the model's
    features.
    +
    Every feature of the model needs to be mapped to a vertex attribute.
  '''


def exposeInternalEdgeId(*args, **kwargs):
  '''  Exposes the internal edge ID as an attribute. Useful if you want to identify edges, for example in
  an exported dataset.

  :param name:   The ID attribute will be saved under this name.
  '''


def copyVertexAttributesFromSegmentation(*args, **kwargs):
  '''  Copies all vertex attributes from the segmentation to the parent.

  This operation is only available when each vertex belongs to just one segment.
  (As in the case of connected components, for example.)

  :param prefix:   A prefix for the new attribute names. Leave empty for no prefix.
  '''


def segmentByStringAttribute(*args, **kwargs):
  '''  Segments the vertices by a `String` vertex attribute.

  Every vertex with the same attribute value will belong to one segment.

  :param name:   The new segmentation will be saved under this name.
  :param attr:   The `String` attribute to segment by.
  '''


def sql6(*args, **kwargs):
  '''  Executes an SQL query on its six inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`, `four`, `five`, `six`. For example:

  ```
  select * from one
  union select * from two
  union select * from three
  union select * from four
  union select * from five
  union select * from six
  ```

  :prefix: one.
  :maybe-tick: {backtick}
  include::{g}[tag


  '''


def sql7(*args, **kwargs):
  '''  Executes an SQL query on its seven inputs, which can be either graphs or tables. Outputs a table.
  The inputs are available in the query as `one`, `two`, `three`, `four`, `five`, `six`, `seven`.
  For example:

  ```
  select * from one
  union select * from two
  union select * from three
  union select * from four
  union select * from five
  union select * from six
  union select * from seven
  ```

  :prefix: one.
  :maybe-tick: {backtick}
  include::{g}[tag


  '''


def weightedAggregateVertexAttributeGlobally(*args, **kwargs):
  '''  Aggregates vertex attributes across the entire graph into one graph attribute for each attribute.
  For example you could use it to calculate the average age across an entire dataset of people
  weighted by their PageRank.

  :param prefix:   Save the aggregated values with this prefix.
  :param weight:   The `number` attribute to use as weight.

    include::{g}
  '''


def trainAGcnClassifier(*args, **kwargs):
  '''  Trains a https://tkipf.github.io/graph-convolutional-networks/[Graph Convolutional Network]
  using https://pytorch-geometric.readthedocs.io/en/latest/[Pytorch Geometric].
  Applicable for classification problems.

  :param save_as:   The resulting model will be saved as a graph attribute using this name.
  :param iterations:   Number of training iterations.
  :param features:   Vector attribute containing the features to be used as inputs for the training algorithm.
  :param label:   The attribute we want to predict.
  :param forget:   Set true to allow a vertex to see the labels of its neighbors and use them for
    predicting its own label.
  :param batch_size:   In each iteration of the training, we compute the error only on a subset of the vertices.
    Batch size specifies the size of this subset.
  :param learning_rate:   Value of the learning rate.
  :param hidden_size:   Size of the hidden layers.
  :param num_conv_layers:   Number of convolution layers.
  :param conv_op:   The type of graph convolution to use.
    https://pytorch-geometric.readthedocs.io/en/latest/modules/nn.html#torch_geometric.nn.conv.GCNConv
  :param seed:   Random seed for initializing network weights and choosing training batches.
  '''


def pullSegmentationOneLevelUp(*args, **kwargs):
  '''  Creates a copy of a segmentation in the parent of its parent segmentation.
  In the created segmentation, the set of segments will be the same as in the
  original. A vertex will be made member of a segment if it was transitively
  member of the corresponding segment in the original segmentation. The attributes
  and sub-segmentations of the segmentation are also copied.


  '''


def graphRejoin(*args, **kwargs):
  '''  This operation allows the user to join (i.e., carry over) attributes from one graph to another one.
  This is only allowed when the target of the join (where the attributes are taken to) and the source
  (where the attributes are taken from) are compatible. Compatibility in this context means that
  the source and the target have a "common ancestor", which makes it possible to perform the join.
  Suppose, for example, that operation <<take-edges-as-vertices>> have been applied, and then some
  new vertex attributes have been computed on the resulting graph. These new vertex
  attributes can now be joined back to the original graph (that was the input for
  <<take-edges-as-vertices>>), because there is a correspondence between the edges of the
  original graph and the vertices that contain the newly computed vertex attributes.

  Conversely, the edges and the vertices of a graph will not be compatible (even
  if the number of edges is the same as the number of vertices), because no such
  correspondence can be established between the edges and the vertices in this case.

  Additionally, it is possible to join segmentations from another graph. This operation
  has an additional requirement (besides compatibility), namely, that both the target of the
  join (the left side) and the source be vertices (and not edges).

  Please, bear it in mind that both attributes and segmentations will overwrite
  the original attributes and segmentations on the right side in case there is
  a name collision.

  When vertex attributes are joined, it is also possible to copy over the edges from
  the source graph (provided that the source graph has edges). In this case, the
  original edges in the target graph are dropped, and the source edges (along with
  their attributes) will take their place.

  :param attrs:   Attributes that should be joined to the graph. They overwrite attributes in the
    target graph which have identical names.
  :param segs:   Segmentations to join to the graph. They overwrite segmentations in the target
    side of the graph which have identical names.
  :param edge:   When set, the edges of the source graph (and their attributes) will replace
    the edges of the target graph.
  '''


def trainLinearRegressionModel(*args, **kwargs):
  '''  Trains a linear regression model using the graph's vertex attributes.

  :param name:   The model will be stored as a graph attribute using this name.
  :param label:   The vertex attribute for which the model is trained.
  :param features:   Attributes to be used as inputs for the training algorithm. The trained model
    will have a list of features with the same names and semantics.
  :param method:   The algorithm used to train the linear regression model.
  '''


def weightedAggregateFromSegmentation(*args, **kwargs):
  '''  Aggregates vertex attributes across all the segments that a vertex in the base graph belongs to.
  For example, it can calculate an average over the cliques a person belongs to, weighted by
  the size of the cliques.

  :param prefix:   Save the aggregated attributes with this prefix.
  :param weight:   The `number` attribute to use as weight.

    include::{g}
  '''


def addRandomVertexAttribute(*args, **kwargs):
  '''  include::{g}[tag


  '''


def renameEdgeAttributes(*args, **kwargs):
  '''  Changes the name of edge attributes.

  :param title:   If the new name is empty, the attribute will be discarded.
  '''


def aggregateToSegmentation(*args, **kwargs):
  '''  Aggregates vertex attributes across all the vertices that belong to a segment.
  For example, it can calculate the average age of each clique.


  '''


def useBaseGraphAsSegmentation(*args, **kwargs):
  '''  Creates a new segmentation which is a copy of the base graph. Also creates segmentation links
  between the original vertices and their corresponding vertices in the segmentation.

  For example, let's say we have a social network and we want to make a segmentation containing a
  selected group of people and the segmentation links should represent the original connections
  between the members of this selected group and other people.

  We can do this by first using this operation to copy the base graph to segmentation then using
  the <<Grow segmentation>> operation to add the necessary segmentation links. Finally, using the
  <<Filter by attributes>> operation, we can ensure that the segmentation contains only members of
  the selected group.

  :param name:   The name assigned to the new segmentation. It defaults to the graph's name.
  '''


def createGraphInPython(*args, **kwargs):
  '''  Executes custom Python code to define a graph.
  Ideal for creating complex graphs programmatically and for loading
  datasets in non-standard formats.

  The following example creates a small graph with some attributes.

  [source,python]
  ----
  vs

  :param code:   The Python code you want to run. See the operation description for details.
  :param outputs:   A comma-separated list of attributes that your code generates.
    These must be annotated with the type of the attribute.
    For example, `vs.my_new_attribute: str, vs.another_new_attribute: float, graph_attributes.also_new: str`.
  '''


def createEdgesFromSetOverlaps(*args, **kwargs):
  '''  Connects segments with large enough overlaps.

  :param minoverlap:   Two segments will be connected if they have at least this many members in common.
  '''


def combineSegmentations(*args, **kwargs):
  '''  Creates a new segmentation from the selected existing segmentations.
  Each new segment corresponds to one original segment from each of the original
  segmentations, and the new segment is the intersection of all the corresponding
  segments. We keep non-empty resulting segments only. Edges between segmentations
  are discarded.

  If you have segmentations A and B with two segments each, such as:

   - A

  :param name:   The new segmentation will be saved under this name.
  :param segmentations:   The segmentations to combine. Select two or more.
  '''


def discardEdgeAttributes(*args, **kwargs):
  '''  Throws away edge attributes.

  :param name:   The attributes to discard.
  '''


def fillEdgeAttributesWithConstantDefaultValues(*args, **kwargs):
  '''  An attribute may not be defined on every edge. This operation sets a default value
  for the edges where it was not defined.

  :param title:   The given value will be set for edges where the attribute is not defined. No change for
    attributes for which the default value is left empty. The default value
    must be numeric for `number` attributes.
  '''


def exposeInternalVertexId(*args, **kwargs):
  '''  Exposes the internal vertex ID as an attribute. This attribute is automatically generated
  by operations that generate new vertex sets. (In most cases this is already available as attribute ‘id’.)
  But you can regenerate it with this operation if necessary.

  :param name:   The ID attribute will be saved under this name.
  '''


def anchor(*args, **kwargs):
  '''  This special box represents the workspace itself. There is always exactly one instance of it. It
  allows you to control workspace-wide settings as parameters on this box. It can also serve to anchor
  your workspace with a high-level description.

  :param description:   An overall description of the purpose of this workspace.
  :param parameters:   Workspaces containing output boxes can be used as <<custom-boxes, custom boxes>> in other
    workspaces. Here you can define what parameters the custom box created from this workspace shall
    have.
    +
    Parameters can also be used as workspace-wide constants. For example if you want to import
    `accounts-2017.csv` and `transactions-2017.csv`, you could create a `date` parameter with default
    value set to `2017` and import the files as `accounts-$date.csv` and `transactions-$date.csv`. (Make
    sure to mark these parametric file names as <<parametric-parameters, parametric>>.)
    This makes it easy to change the date for all imported files at once later.
  '''


def saveToSnapshot(*args, **kwargs):
  '''  Saves the input to a snapshot. The location of the snapshot has to be specified as
  a full path.

  include::{g}[tag

  :param path:   The full path of the target snapshot in the LynxKite directory system.
  '''


def lookupRegion(*args, **kwargs):
  '''  For every `position` vertex attribute looks up features in a Shapefile and returns a specified
  attribute.

  * The lookup depends on the coordinate reference system of the feature. The input position must
    use the same coordinate reference system as the one specified in the Shapefile.
  * If there are no matching features the output is omitted.
  * If the specified attribute does not exist for any matching feature the output is omitted.
  * If there are multiple suitable features the algorithm picks the first one.

  Shapefiles can be obtained from various sources, like
  http://wiki.openstreetmap.org/wiki/Shapefiles[OpenStreetMap].

  :param position:   The (latitude, longitude) location tuple.
  :param shapefile:   The https://en.wikipedia.org/wiki/Shapefile
  :param attribute:   The attribute in the Shapefile used for the output.
  :param ignoreUnsupportedShapes:   If set `true`, silently ignores unknown shape types potentially contained by the Shapefile.
    Otherwise throws an error.
  :param output:   The name of the new vertex attribute.
  '''


def replaceEdgesWithTriadicClosure(*args, **kwargs):
  '''  For every A{to}B{to}C triplet, creates an A{to}C edge. The original edges are discarded.
  The new A{to}C edge gets the attributes of the original A{to}B and B{to}C edges with prefixes "ab_" and "bc_".

  Be aware, in dense graphs a plenty of new edges can be generated.

  Possible use case: we are looking for connections between vertices, like same subscriber with multiple devices.
  We have an edge metric that we think is a good indicator, or we have a model that gives predictions for edges.
  If we want to calculate this metric, and pick the edges with high values, it is possible that the edge
  that would be the winner does not exist.
  Often we think that a transitive closure would add the missing edge.
  For example, I don't call my second phone, but I call a lot of the same people from the two phones.


  '''


def importWellKnownGraphDataset(*args, **kwargs):
  '''  Gives easy access to graph datasets commonly used for benchmarks.

  See the https://pytorch-geometric.readthedocs.io/en/1.4.1/modules/datasets.html[PyTorch Geometric documentation]
  for details about the specific datasets.

  :param name:   Which dataset to import.
  '''


def comment(*args, **kwargs):
  '''  Adds a comment to the workspace. As with any box, you can freely place your comment anywhere on the
  workspace. Adding comments does not have any effect on the computation but can potentially make your
  workflow easier to understand for others -- or even for your future self.

  https://en.wikipedia.org/wiki/Markdown#Example[Markdown] can be used to present formatted text or
  embed links and images.

  :param comment:   Markdown text to be displayed in the workspace.
  '''


def segmentByInterval(*args, **kwargs):
  '''  Segments the vertices by a pair of `number` vertex attributes representing intervals.

  The domain of the attributes is split into intervals of the given size. Each of these
  intervals will represent a segment. Each vertex will belong to each segment whose
  interval intersects with the interval of the vertex. Empty segments are not created.

  :param name:   The new segmentation will be saved under this name.
  :param begin_attr:   The `number` attribute corresponding the beginning of intervals to segment by.
  :param end_attr:   The `number` attribute corresponding the end of intervals to segment by.
  :param interval_size:   The attribute's domain will be split into intervals of this size. The splitting always starts at
    zero.
  :param overlap:   If you enable overlapping intervals, then each interval will have a 50% overlap
    with both the previous and the next interval.
  '''


def predictWithGcn(*args, **kwargs):
  '''  Uses a trained https://tkipf.github.io/graph-convolutional-networks/[Graph Convolutional Network]
  to make predictions.

  :param save_as:   The prediction will be saved as an attribute under this name.
  :param features:   Vector attribute containing the features to be used as inputs for the algorithm.
  :param label:   The attribute we want to predict. (This is used if the model was trained to use
    the target labels as additional inputs.)
  :param model:   The model to use for the prediction.
  '''


def segmentByVectorAttribute(*args, **kwargs):
  '''  Segments the vertices by a vector vertex attribute.

  Segments are created from the values in all of the vector attributes. A vertex is connected
  to every segment corresponding to the elements in the vector.

  :param name:   The new segmentation will be saved under this name.
  :param attr:   The vector attribute to segment by.
  '''


def checkCliques(*args, **kwargs):
  '''  Validates that the segments of the segmentation are in fact cliques.

  Creates a new `invalid_cliques` graph attribute, which lists non-clique segment IDs up to a certain number.

  :param selected:   The validation can be restricted to a subset of the segments, resulting in quicker operation.
  :param bothdir:   Whether edges have to exist in both directions between all members of a clique.
  '''


def sql1(*args, **kwargs):
  '''  Executes a SQL query on a single input, which can be either a graph or a table. Outputs a table.
  If the input is a table, it is available in the query as `input`. For example:

  ```
  select * from input
  ```

  If the input is a graph, its internal tables are available directly.

  :prefix:
  :maybe-tick:
  include::{g}[tag


  '''


def exportToJdbc(*args, **kwargs):
  '''  JDBC is used to connect to relational databases such as MySQL. See <<jdbc-details>> for setup steps
  required for connecting to a database.

  :param url:   The connection URL for the database. This typically includes the username and password. The exact
    syntax entirely depends on the database type. Please consult the documentation of the database.
  :param table:   The name of the database table to export to.
  :param mode:   Describes whether LynxKite should expect a table to already exist and how to handle this case.
    +
    **The table must not exist** means the table will be created and it is an error if it already
    exists.
    +
    **Drop the table if it already exists** means the table will be deleted and re-created if
    it already exists. Use this mode with great care.
    +
    **Insert into an existing table** requires the
    table to already exist and it will add the exported data at the end of the existing table.
  '''


def predictWithModel(*args, **kwargs):
  '''  Creates predictions from a model and vertex attributes of the graph.

  :param name:   The new attribute of the predictions will be created under this name.
  :param model:   The model used for the predictions and a mapping from vertex attributes to the model's
    features.
    +
    Every feature of the model needs to be mapped to a vertex attribute.
  '''


def renameVertexAttributes(*args, **kwargs):
  '''  Changes the name of vertex attributes.

  :param title:   If the new name is empty, the attribute will be discarded.
  '''


def copySegmentation(*args, **kwargs):
  '''  Creates a copy of a segmentation.

  :param from:
  :param to:
  '''


def computeDegree(*args, **kwargs):
  '''  For every vertex, this operation calculates either the number of edges it is connected to
  or the number of neighboring vertices it is connected to.
  You can use the `Count` parameter to control this calculation:
  choosing one of the 'edges' settings can result in a neighboring
  vertex being counted several times (depending on the number of edges between
  the vertex and the neighboring vertex); whereas choosing one of the 'neighbors' settings
  will result in each neighboring vertex counted once.

  :param name:   The new attribute will be created under this name.
  :param direction:   - `incoming edges`: Count the edges coming in to each vertex.
     - `outgoing edges`: Count the edges going out of each vertex.
     - `all edges`: Count all the edges going in or out of each vertex.
     - `symmetric edges`:
       Count the 'symmetric' edges for each vertex: this means that if you have n edges
       going from A to B and k edges going from B to A, then min(n,k) edges will be
       taken into account for both A and B.
     - `in-neighbors`: For each vertex A, count those vertices
       that have an outgoing edge to A.
     - `out-neighbors`: For each vertex A, count those vertices
       that have an incoming edge from A.
     - `all neighbors`: For each vertex A, count those vertices
       that either have an outgoing edge to or an incoming edge from A.
     - `symmetric neighbors`: For each vertex A, count those vertices
       that have both an outgoing edge to and an incoming edge from A.
  '''


def addRankAttribute(*args, **kwargs):
  '''  Creates a new vertex attribute that is the _rank_ of the vertex when ordered by the key
  attribute. Rank 0 will be the vertex with the highest or lowest key attribute value
  (depending on the direction of the ordering). `String` attributes will be ranked
  alphabetically.

  This operation makes it easy to find the top (or bottom) N vertices by an attribute.
  First, create the ranking attribute. Then filter by this attribute.

  :param rankattr:   The new attribute will be created under this name.
  :param keyattr:   The attribute to rank by.
  :param order:   With _ascending_ ordering rank 0 belongs to the vertex with the minimal key attribute value or
    the vertex that is at the beginning of the alphabet.
    With _descending_ ordering rank 0 belongs to the vertex with the maximal key attribute value or
    the vertex that is at the end of the alphabet.
  '''


def importJdbc(*args, **kwargs):
  '''  JDBC is used to connect to relational databases such as MySQL. See <<jdbc-details>> for setup steps
  required for connecting to a database.

  :param jdbc_url:   The connection URL for the database. This typically includes the username and password. The exact
    syntax entirely depends on the database type. Please consult the documentation of the database.
  :param jdbc_table:   The name of the database table to import.
    +
    All identifiers have to be properly quoted according to the SQL syntax of the source database.
    +
    The following formats may work depending on the type of the source database:
    +
    * `TABLE_NAME`
    * `SCHEMA_NAME.TABLE_NAME`
    * `(SELECT * FROM TABLE_NAME WHERE <filter condition>) TABLE_ALIAS`
    +
    In the last example the filtering query runs on the source database,
    before the import. It can dramatically reduce network
    traffic needed for the import operation and it makes possible to use data source
    specific SQL dialects.
  :param key_column:   This column is used to partition the SQL query. The range from `min(key)` to `max(key)`
    will be split into a sub-range for each Spark worker, so they can each query a part of the data in
    parallel.
    +
    Pick a column that is uniformly distributed. Numerical identifiers will give the best performance.
    String (`VARCHAR`) columns are also supported but only work well if they mostly contain letters of
    the English alphabet and numbers.
    +
    If the partitioning column is left empty, only a fraction of the cluster resources will be used.
    +
    The column name has to be properly quoted according to the SQL syntax of the source database.
  :param num_partitions:   LynxKite will perform this many SQL queries in parallel to get the data. Leave at zero to let
    LynxKite automatically decide. Set a specific value if the database cannot support that many
    queries.
  :param partition_predicates:   This advanced option provides even greater control over the partitioning. It is an alternative
    option to specifying the key column. Here you can specify a comma-separated list of `WHERE` clauses,
    which will be used as the partitions.
    +
    For example you could provide `AGE < 30, AGE >
  '''


def computeEmbeddedness(*args, **kwargs):
  '''  https://arxiv.org/abs/1009.1686[Edge embeddedness] is the overlap size of vertex neighborhoods along
  the edges. If an A{to}B edge has an embeddedness of `N`, it means A and B have `N` common neighbors.

  :param name:   The new attribute will be created under this name.
  '''


def createVertices(*args, **kwargs):
  '''  Creates a new vertex set with no edges. Two attributes are generated: `id` and `ordinal`. `id`
  is the internal vertex ID, while `ordinal` is an index for the vertex: it goes from zero to the
  vertex set size.

  :param size:   The number of vertices to create.
  '''


def exportToCsv(*args, **kwargs):
  '''  CSV stands for comma-separated values. It is a common human-readable file format where each record
  is on a separate line and fields of the record are simply separated with a comma or other delimiter.
  CSV does not store data types, so all fields become strings when importing from this format.

  :param path:   The distributed file-system path of the output file. It defaults to `<auto>`, in which case the
    path is auto generated from the parameters and the type of export (e.g. `Export to CSV`).
    This means that the same export operation with the same parameters always generates the same path.
  :param delimiter:   The delimiter separating the fields in each line.
  :param quote:   The character used for quoting strings that contain the delimiter. If the string also contains the
    quote character, it will be escaped with a backslash (`{backslash}`).
  :param quote_all:   Quotes all string values if set. Only quotes in the necessary cases otherwise.
  :param header:   Whether or not to include the header in the CSV file. If the data is exported as multiple CSV files
    the header will be included in each of them. When such a data set is directly downloaded, the header
    will appear multiple times in the resulting file.
  :param escape:   The character used for escaping quotes inside an already quoted value.
  :param null_value:   The string representation of a `null` value. This is how `null`-s are going to be written in
    the CSV file.
  :param date_format:   The string that indicates a date format. Custom date formats follow the formats at
    https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  :param timestamp_format:   The string that indicates a timestamp format. Custom date formats follow the formats at
    https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  :param drop_leading_white_space:   A flag indicating whether or not leading whitespaces from values being written should be skipped.
  :param drop_trailing_white_space:   A flag indicating whether or not trailing whitespaces from values being written should be skipped.
  :param version:   Version is the version number of the result of the export operation. It is a non negative integer.
    LynxKite treats export operations as other operations: it remembers the result (which in this case
    is the knowledge that the export was successfully done) and won't repeat the calculation. However,
    there might be a need to export an already exported table with the same set of parameters (e.g. the
    exported file is lost). In this case you need to change the version number, making that parameters
    are not the same as in the previous export.
  :param for_download:   Set this to "true" if the purpose of this export is file download: in this case LynxKite will
    repartition the data into one single file, which will be downloaded. The default "no" will
    result in no such repartition: this performs much better when other, partition-aware tools
    are used to import the exported data.


    include::{g}
  '''


def segmentByDoubleAttribute(*args, **kwargs):
  '''  Segments the vertices by a `number` vertex attribute.

  The domain of the attribute is split into intervals of the given size and every vertex that
  belongs to a given interval will belong to one segment. Empty segments are not created.

  :param name:   The new segmentation will be saved under this name.
  :param attr:   The `number` attribute to segment by.
  :param interval_size:   The attribute's domain will be split into intervals of this size. The splitting always starts at
    zero.
  :param overlap:   If you enable overlapping intervals, then each interval will have a 50% overlap
    with both the previous and the next interval. As a result each vertex will belong
    to two segments, guaranteeing that any vertices with an attribute value difference
    less than half the interval size will share at least one segment.
  '''


def copyVertexAttribute(*args, **kwargs):
  '''  Creates a copy of a vertex attribute.

  :param from:
  :param to:
  '''


def predictEdgesWithHyperbolicPositions(*args, **kwargs):
  '''  Creates additional edges in a graph based on
  hyperbolic distances between vertices.
   _2 * size_ edges will be added because
  the new edges are undirected.
  Vertices must have two _number_ vertex attributes to be
  used as radial and angular coordinates.

  The algorithm is based on
  https://arxiv.org/abs/1106.0286[Popularity versus Similarity in Growing Networks] and
  https://arxiv.org/abs/1205.4384[Network Mapping by Replaying Hyperbolic Growth].

  :param size:   The number of edges to generate.
    The total number will be _2 * size_ because every
    edge is added in two directions.
  :param externaldegree:   The number of edges a vertex creates from itself
     upon addition to the growth simulation graph.
  :param internaldegree:   The average number of edges created between older vertices whenever
    a new vertex is added to the growth simulation graph.
  :param exponent:   The exponent of the power-law degree distribution.
    Values can be 0.5 - 1, endpoints excluded.
  :param radial:   The vertex attribute to be used as radial coordinates.
    Should not contain negative values.
  :param angular:   The vertex attribute to be used as angular coordinates.
    Values should be 0 - 2 * Pi.
  '''


def computeClusteringCoefficient(*args, **kwargs):
  '''  Calculates the local
  http://en.wikipedia.org/wiki/Clustering_coefficient[clustering
  coefficient] attribute for every vertex. It quantifies how close the
  vertex's neighbors are to being a clique. In practice a high (close to
  1.0) clustering coefficient means that the neighbors of a vertex are
  highly interconnected, 0.0 means there are no edges between the
  neighbors of the vertex.

  :param name:   The new attribute will be created under this name.
  '''


def useMetagraphAsGraph(*args, **kwargs):
  '''  Loads the relationships between LynxKite entities such as attributes and operations as a graph.
  This complex graph can be useful for debugging or demonstration purposes. Because it exposes
  data about all graphs, it is only accessible to administrator users.

  :param timestamp:   This number will be used to identify the current state of the metagraph. If you edit the history
    and leave the timestamp unchanged, you will get the same metagraph as before. If you change the
    timestamp, you will get the latest version of the metagraph.
  '''


def computeInPython(*args, **kwargs):
  '''  Executes custom Python code to define new vertex, edge, or graph attributes.

  The following example computes two new vertex attributes (`with_title` and `age_squared`),
  two new edge attributes (`score` and `names`), and two new graph_attributes (`hello` and `average_age`).
  (You can try it on the <<Create example graph, example graph>> which
  has the attributes used in this code.)

  [source,python]
  ----
  vs['with_title']

  :param code:   The Python code you want to run. See the operation description for details.
  :param inputs:   A comma-separated list of attributes that your code wants to use.
    For example, `vs.my_attribute, vs.another_attribute, graph_attributes.last_one`.
  :param outputs:   A comma-separated list of attributes that your code generates.
    These must be annotated with the type of the attribute.
    For example, `vs.my_new_attribute: str, vs.another_new_attribute: float, graph_attributes.also_new: str`.
  '''


def convertEdgeAttributeToString(*args, **kwargs):
  '''  Converts the selected edge attributes to `String` type.

  The attributes will be converted in-place. If you want to keep the original String attribute as
  well, make a copy first!

  :param attr:   The attributes to be converted.
  '''


def externalComputation3(*args, **kwargs):
  '''  include::{g}[tag


  '''


def externalComputation2(*args, **kwargs):
  '''  include::{g}[tag


  '''


def graphVisualization(*args, **kwargs):
  '''  Creates a visualization from the input graph. You can use the box parameter popup to
  define the parameters and layout of the visualization. See <<graph-visualizations>> for more details.


  '''


def externalComputation10(*args, **kwargs):
  '''  include::{g}[tag


  '''


def importFromHive(*args, **kwargs):
  '''  Import an https://hive.apache.org/[Apache Hive] table directly to LynxKite.

  :param table-name:   The name of the Hive table to import.

    include::{g}
  '''


def importUnionOfTableSnapshots(*args, **kwargs):
  '''  Makes the union of a list of previously saved table snapshots accessible from the workspace
  as a single table.

  The union works as the UNION ALL command in SQL and does not remove duplicates.

  :param paths:   The comma separated set of full paths to the snapshots in LynxKite's virtual filesystem.

     - Each path has to refer to a table snapshot.
     - The tables have to have the same schema.
     - The output table will union the input tables in the same order as defined here.
  '''


def embedVertices(*args, **kwargs):
  '''  Creates a vertex embedding using the
  https://pytorch-geometric.readthedocs.io/en/1.4.1/modules/nn.html#torch_geometric.nn.models.Node2Vec[PyTorch Geometric implementation]
  of the https://arxiv.org/abs/1607.00653[node2vec] algorithm.

  :param save_as:   The new attribute will be created under this name.
  :param iterations:   Number of training iterations.
  :param dimensions:   The size of each embedding vector.
  :param walks_per_node:   Number of random walks collected for each vertex.
  :param walk_length:   Length of the random walks collected for each vertex.
  :param context_size:   The random walks will be cut with a rolling window of this size.
    This allows reusing the same walk for multiple vertices.
  '''


def convertEdgeAttributeToDouble(*args, **kwargs):
  '''  Converts the selected `String` typed edge attributes to the `number` type.

  The attributes will be converted in-place. If you want to keep the original `String` attribute as
  well, make a copy first!

  :param attr:   The attributes to be converted.
  '''


def discardLoopEdges(*args, **kwargs):
  '''  Discards edges that connect a vertex to itself.


  '''


def aggregateFromSegmentation(*args, **kwargs):
  '''  Aggregates vertex attributes across all the segments that a vertex in the base graph belongs to.
  For example, it can calculate the average size of cliques a person belongs to.

  :param prefix:   Save the aggregated attributes with this prefix.

    include::{g}
  '''


def splitVertices(*args, **kwargs):
  '''  Split (multiply) vertices in a graph. A numeric vertex attribute controls how many
  copies of the vertex should exist after the operation. If this attribute is
  1, the vertex will be kept as it is. If this attribute is zero, the vertex
  will be discarded entirely. Higher values (e.g., 2) will result in
  more identical copies of the given vertex.
  All edges coming from and going to this vertex are
  multiplied (or discarded) appropriately.

  After the operation, all previous vertex and edge attributes will be preserved;
  in particular, copies of one vertex will have the same values for the previous vertex
  attributes. A new vertex attribute (the so called index attribute) will also be
  created so that you can differentiate between copies of the same vertex.
  If a given vertex was multiplied by n times, the n new vertices will have n different
  index attribute values running from 0 to n-1.

  This operation assigns new vertex ids to the vertices; these will be accessible
  via a new vertex attribute.

  :param rep:   A numberic vertex attribute that specifies how many copies of the vertex should
    exist after the operation.
    (The number value is rounded to the nearest integer, so 1.8 will mean 2 copies.)
  :param idx:   The name of the attribute that will contain unique identifiers for the otherwise
    identical copies of the vertex.
  '''


def renameSegmentation(*args, **kwargs):
  '''  Changes the name of a segmentation.

  This operation is more easily accessed from the segmentation's dropdown menu in the graph state view.

  :param from:   The segmentation to rename.
  :param to:   The new name.
  '''


def useTableAsGraph(*args, **kwargs):
  '''  Imports edges from a table. Each line in the table represents one edge.
  Each column in the table will be accessible as an edge attribute.

  Vertices will be generated for the endpoints of the edges with two vertex attributes:

   - `stringId` will contain the ID string that was used in the table.
   - `id` will contain the internal vertex ID.

  This is useful when your table contains edges (e.g., calls) and there is no separate
  table for vertices. This operation makes it possible to load edges and use them
  as a graph. Note that this graph will never have zero-degree vertices.

  :param src:
  :param dst:
  '''


def takeEdgesAsVertices(*args, **kwargs):
  '''  Takes a graph and creates a new one where the vertices correspond to the original graph's
  edges. All edge attributes in the original graph are converted to vertex attributes in the new
  graph with the `edge_` prefix. All vertex attributes are converted to two vertex attributes with
  `src_` and `dst_` prefixes. Segmentations of the original graph are lost.


  '''


def addPopularityXSimilarityOptimizedEdges(*args, **kwargs):
  '''  <<experimental-operation,+++<i class

  :param externaldegree:   The number of edges a vertex creates from itself upon addition to the growing graph.
  :param internaldegree:   The average number of edges created between older vertices whenever a new vertex
     is added to the growing graph.
  :param exponent:   The exponent of the power-law degree distribution. Values can be 0.5 - 1, endpoints excluded.
  :param seed:   The random seed.
    +
    include::{g}
  '''


def bundleVertexAttributesIntoAVector(*args, **kwargs):
  '''  Bundles the chosen `number` and `Vector[number]` attributes into one `Vector` attribute.
  By default, LynxKite puts the numeric attributes after each other in alphabetical order and
  then concatenates the Vector attributes to the resulting Vector in alphabetical
  order as well. The resulting attribute is undefined where any of the input attributes
  is undefined.

  For example, if you bundle the `age`, `favorite_day` and `income` attributes into a `Vector` attribute
  called `everything`, you end up with the following attributes.

  |

  :param output:   The new attribute will be created under this name.
  :param elements:   The attributes you would like to bundle into a Vector.
  '''


def addRandomEdgeAttribute(*args, **kwargs):
  '''  include::{g}[tag


  '''


def externalComputation9(*args, **kwargs):
  '''  include::{g}[tag


  '''


def externalComputation8(*args, **kwargs):
  '''  include::{g}[tag


  '''


def approximateClusteringCoefficient(*args, **kwargs):
  '''  Scalable algorithm to calculate the approximate local
  http://en.wikipedia.org/wiki/Clustering_coefficient[clustering
  coefficient] attribute for every vertex. It quantifies how close the
  vertex's neighbors are to being a clique. In practice a high (close to
  1.0) clustering coefficient means that the neighbors of a vertex are
  highly interconnected, 0.0 means there are no edges between the
  neighbors of the vertex.

  :param name:   The new attribute will be created under this name.
  :param bits:   This algorithm is an approximation. This parameter sets the trade-off between
    the quality of the approximation and the memory and time consumption of the algorithm.
  '''


def reverseEdgeDirection(*args, **kwargs):
  '''  Replaces every A{to}B edge with its reverse edge (B{to}A).

  Attributes are preserved. Running this operation twice gets back the original graph.


  '''
