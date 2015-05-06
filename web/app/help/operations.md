# Operations

## Discard vertices

Throws away all vertices. This implies discarding all edges, attributes, and segmentations too.

## Discard edges

Throws away all edges. This implies discarding all edge attributes too.

## New vertex set

Creates a new vertex set with no edges. Two attributes are generated, `id` and `ordinal`. `id`
is the internal vertex ID, while `ordinal` is an index for the vertex: it goes from zero to the
vertex set size.

---
 - <span class="param" name="size">Vertex set size</span>: The number of vertices to create.

## Create random edge bundle

Creates edges randomly, so that each vertex will have a degree uniformly chosen between 0 and 2 ×
the provided parameter.

---
 - <span class="param" name="degree">Average degree</span>: The degree of a vertex will be chosen
   uniformly between 0 and 2 × this number. This results in generating (number of vertices ×
   average degree) edges.
 - <span class="param" name="seed">Seed</span>: The random seed.

   ---
   LynxKite operations are typically deterministic. If you re-run an operation with the same random
   seed, you will get the same results as before. To get a truly independent random re-run, make
   sure you choose a different random seed.

   The default value for random seed parameters is randomly picked, so only very rarely do you need
   to give random seeds any thought.

## Connect vertices on attribute

Creates edges between vertices that are equal in a chosen attribute. If the source attribute of A
equals the destination attribute of B, an A&nbsp;&rarr;&nbsp;B edge will be generated.

The two attributes must be of the same data type.

---
 - <span class="param" name="fromAttr">Source attribute</span>: An A&nbsp;&rarr;&nbsp;B edge is
   generated when this attribute on A matches the destination attribute on B.
 - <span class="param" name="toAttr">Destination attribute</span>: An A&nbsp;&rarr;&nbsp;B edge is
   generated when the source attribute on A matches this attribute on B.

## Import vertices from CSV files

Imports vertices (no edges) from a CSV file, or files.
Each field in the CSV will be accessible as a vertex attribute.
An extra vertex attribute is generated to hold the internal vertex ID.

Wildcard (`foo/*.csv`) and glob (`foo/{bar,baz}.csv`) patterns are accepted.
S3 paths must include the key name and secret key in the following format:
`s3n://key_name:secret_key@bucket/dir/file`

## Import vertices from a database

Imports vertices (no edges) from a SQL database.
An extra vertex attribute is generated to hold the internal vertex ID.

The database name is the JDBC connection string without the `jdbc:` prefix.
(For example `mysql://127.0.0.1/?user=batman&password=alfred`.)
An integer column must be specified as the key, and you have to select a key range.

## Import edges for existing vertices from CSV files

Imports edges from a CSV file, or files. Your vertices must have a key attribute, by which
the edges can be attached to them.

Wildcard (`foo/*.csv`) and glob (`foo/{bar,baz}.csv`) patterns are accepted.
S3 paths must include the key name and secret key in the following format:
`s3n://key_name:secret_key@bucket/dir/file`

## Import edges for existing vertices from a database

Imports edges from a SQL database. Your vertices must have a key attribute, by which
the edges can be attached to them.

The database name is the JDBC connection string without the `jdbc:` prefix.
(For example `mysql://127.0.0.1/?user=batman&password=alfred`.)
An integer column must be specified as the key, and you have to select a key range.

## Import vertices and edges from single CSV fileset

Imports edges from a CSV file, or files. Each field in the CSV will be accessible as an edge
attribute.

Vertices will be generated for the endpoints of the edges with two vertex attributes:
 - `stringID` will contain the ID string that was used in the CSV.
 - `id` will contain the internal vertex ID.

Wildcard (`foo/*.csv`) and glob (`foo/{bar,baz}.csv`) patterns are accepted.
S3 paths must include the key name and secret key in the following format:
`s3n://key_name:secret_key@bucket/dir/file`

## Import vertices and edges from single database table

Imports edges from a SQL database. Each column in the table will be accessible as an edge
attribute.

Vertices will be generated for the endpoints of the edges with two vertex attributes:
 - `stringID` will contain the ID string that was used in the CSV.
 - `id` will contain the internal vertex ID.

The database name is the JDBC connection string without the `jdbc:` prefix.
(For example `mysql://127.0.0.1/?user=batman&password=alfred`.)
An integer column must be specified as the key, and you have to select a key range.

## Import vertex attributes from CSV files

Imports vertex attributes for existing vertices from a CSV file.

Wildcard (`foo/*.csv`) and glob (`foo/{bar,baz}.csv`) patterns are accepted.
S3 paths must include the key name and secret key in the following format:
`s3n://key_name:secret_key@bucket/dir/file`

## Import vertex attributes from a database

Imports vertex attributes for existing vertices from a SQL database.

The database name is the JDBC connection string without the `jdbc:` prefix.
(For example `mysql://127.0.0.1/?user=batman&password=alfred`.)
An integer column must be specified as the key, and you have to select a key range.

## Convert vertices into edges

Re-interprets the vertices as edges. You select two string-typed vertex attributes
which specify the source and destination of the edges. New vertices will be generated for each
distinct value of these attributes, and they will be connected if an orignal vertex described
a connection between them.

An example use-case is if your vertices are calls. The converted graph will have subscribers as
its vertices and the calls as its edges.

---
 - <span class="param" name="src">Source</span>: An A&nbsp;&rarr;&nbsp;B edge is
   generated if a vertex exists with the source attribute A and destination attribute B.
 - <span class="param" name="dst">Destination</span>: An A&nbsp;&rarr;&nbsp;B edge is
   generated if a vertex exists with the source attribute A and destination attribute B.

## Maximal cliques

Creates a segmentation of vertices based on the maximal cliques they are the member of.
A maximal clique is a maximal set of vertices where there is an edge between every two vertex.
Since one vertex can be part of multiple maximal cliques this segmentation might be overlapping.

---
 - <span class="param" name="name">Segmentation name</span>: The new segmentation will be saved
   under this name.
 - <span class="param" name="bothdir">Edges required in both directions</span>: Whether edges have
   to exist in both directions between all members of a clique.

   ---
   If the direction of the edges is not important, set this to `false`. This will allow placing two
   vertices into the same clique even if they are only connected in one direction.
 - <span class="param" name="min">Minimum clique size</span>: Cliques smaller than this will not be
   collected.

   ---
   This improves the performance of the algorithm, and small cliques are often not a good indicator
   anyway.
