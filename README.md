# LynxKite Enterprise

[LynxKite](https://lynxkite.com/) is a complete graph data science platform for very large graphs
and other datasets.
It seamlessly combines the benefits of a friendly graphical interface and a powerful Python API.

- Hundreds of scalable **graph operations**, including graph metrics like PageRank, embeddedness,
  and centrality, machine learning methods including
  [GCNs](https://tkipf.github.io/graph-convolutional-networks/), graph segmentations like modular
  clustering, and various transformation tools like aggregations on neighborhoods.
- The two main data types are **graphs and relational tables**. Switch back and forth between the
  two as needed to describe complex logical flows. Run SQL on both.
- A **friendly web UI** for building powerful pipelines of operation boxes. Define your own custom
  boxes to structure your logic.
- Tight integration with **Python** lets you implement custom transformations or create whole
  workflows through a simple API.
- Integrates with the **Hadoop ecosystem**. Import and export from CSV, JSON, Parquet, ORC, JDBC,
  Hive, or Neo4j.
- [Fully documented.](https://lynxkite.com/docs/latest)
- Proven in production on large clusters and real datasets.
- Fully configurable **graph visualizations** and **statistical plots**. Experimental 3D and
  ray-traced graph renderings.

LynxKite is under active development. Check out our [Roadmap](https://lynxkite.com/roadmap) to see
what we have planned for future releases.


## Getting started

Quick try:

```
docker run --rm -p2200:2200 lynxkite/lynxkite
```

Setup with persistent data:

```
docker run \
  -p 2200:2200 \
  -v ~/lynxkite/meta:/metadata -v ~/lynxkite/data:/data \
  -e KITE_MASTER_MEMORY_MB=1024 \
  --name lynxkite lynxkite/lynxkite
```


## Contributing

If you find any bugs, have any questions, feature requests or comments, please
[file an issue](https://github.com/lynxkite/lynxkite/issues/new)
or email us at lynxkite@lynxkite.com.

To build LynxKite you will need:

- [Node.js](https://nodejs.org/) 10+ with [Yarn](http://yarnpkg.org/)
- Java 8 and [SBT](https://www.scala-sbt.org/)
- Python 3.7+ with [Conda](https://docs.conda.io/en/latest/miniconda.html)
- Go 1.14+

Before the first build:

    tools/git/setup.sh
    tools/install_spark.sh
    sphynx/python/install-dependencies.sh
    cp conf/kiterc_template ~/.kiterc

We use `make` for building the whole project.

    make
    stage/bin/lynxkite interactive


## Tests

We have test suits for the different parts of the system:

- **Backend tests** are unit tests for the Scala code. They can also be executed with Sphynx as the
  backend. If you run `make backend-test` it will do both. Or you can start `sbt` and run
  `test-only *SomethingTest` to run just one test. Run `./test_backend.sh -si` to start `sbt` with
  Sphynx as the backend.

- **Frontend tests** use [Protractor](https://www.protractortest.org/) to simulate a user's actions
  on the UI. `make frontend-test` will build everything, start a temporary LynxKite instance and run
  the tests against that. Use `xvfb-run` for headless execution. If you already have a running
  LynxKite instance and you don't mind erasing all data from it, run `npx gulp test` in the `web`
  directory. You can start up a dev proxy that watches the frontend source code for changes with
  `npx gulp serve`. Run the test suite against the dev proxy with `npx gulp test:serve`.

- **Python API tests** are started with `make remote_api-test`. If you already have a running
  LynxKite that is okay to test on, run `python/remote_api/test.sh`. This script can also run a
  subset of the test suite: `python/remote_api/test.sh -p *something*`
