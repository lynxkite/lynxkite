LynxKite
========

## Global setup steps

Install `nvm` (https://github.com/creationix/nvm). Then:

    nvm install 5.7
    nvm alias default 5.7
    npm install -g gulp bower

Install `sbt` (Scala Build Tool):

    echo "deb http://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-get update
    sudo apt-get install sbt

Install Spark:

   - Go to https://spark.apache.org/downloads.html and download appropriate version,
     e.g., spark-1.3.0-bin-hadoop2.4.tgz. (Check the file conf/SPARK_VERSION to see what spark
     version you need - 1.3.0 in this example.) Then:

         cd
         tar xf Downloads/spark-1.3.0-bin-hadoop2.4.tgz
         ln -s spark-1.3.0-bin-hadoop2.4/ spark-1.3.0

For various tools you will require Python and AWS CLI.

Spark does a reverse DNS lookup for 0.0.0.0 on startup. At least on Ubuntu 14.04 this is equivalent
to running `avahi-resolve-address 0.0.0.0` and takes 5 seconds. If you want to avoid this delay on
LynxKite startup, add a line to `/etc/avahi/hosts`:

    0.0.0.0 any.local


## Per repository setup

Set up `git` pre-commit hooks:

    tools/git/setup.sh

Set up web build tools:

    cd web
    npm install
    bower install

## Configure .kiterc

    cp conf/kiterc_template ~/.kiterc
    vi ~/.kiterc # Change defaults if necessary.

## Run LynxKite

To build and run LynxKite invoke the `run.sh` shell script.

## Backend development

You can run `run.sh` all the time, but it will waste time with building the frontend code each time.
The recommended solution is to run `run.sh` or `cd web; gulp; cd -` once to build the frontend
once. Then start `sbt` and run the `stage` command whenever you want to rebuild the backend. In
another terminal you can run `stage/bin/biggraph interactive` to start the server after `stage`.

Also in `sbt` you can run `test` to run the full test suite, or `test-only *SomethingTest` to run
just one test.

## Frontend development

When working on the frontend you can also avoid running `run.sh` all the time. Start LynxKite with
`run.sh` or `stage/bin/biggraph interactive`. Then run `gulp.sh` to start a frontend development
proxy.  If you access LynxKite through the proxy, any changes to frontend files will perform the
necessary frontend build steps and reload the page in the browser.

Given a running backend, frontend tests (Protractor tests) can be run with `cd web; gulp test`. To
run a single test the test code has to be modified. After the one or two `function` parameters add a
`'solo'` parameter to mark the test for solo running. (Multiple tests can be marked with `'solo'` at
the same time.) Run `VERBOSE=true gulp test` to enable verbose mode, which prints the tests names as
it goes. Run `gulp test:serve` to run the tests against the development proxy. This allows for quick
iteration against tests.

By default LynxKite runs on port 2200 and `gulp test` and `gulp serve` look for it on this port. If
you run LynxKite on a different port (say 1234), run `PORT=1234 gulp test` and `PORT=1234 gulp
test`.

The Protractor tests pop up an actual browser. If you want to avoid this, use `xvfb-run gulp test`.
This will run the frontend tests in a virtual framebuffer. (In Ubuntu `xfvb-run` is available in
package `xvfb`.)

The `test_frontend.sh` script builds and starts LynxKite, runs the Protractor tests, then shuts down
LynxKite.

## Big Data tests

If you want to measure the effect of a code change on big data, create a PR and add
a comment to it containing the phrase `Big Data Test please`. This will trigger Jenkins
to spin up an EMR cluster, run the tests and push the results as a commit into your PR.

You can manually run these tests using `test_big_data.sh`, and you can also specify more parameters
that way, e.g. `test_big_data.sh pagerank 'fake_westeros_xt_25m'`. For further use cases, see the
comments in `test_big_data.sh`.


## Run executors on different JVM-s.

Sometimes you want to use a non-local setup on your local machine: e.g., three executors on
three different JVMs, each separate from the driver JVM. This is a better approximation
of a production environment, while still comparatively easy to work with.
Here's how I managed to set it up.

 1. Start spark master:

        ~/spark-<version>/sbin/start-master.sh
    
 2. Start 3 executors:

        SPARK_WORKER_INSTANCES=3 ~/spark-<version>/sbin/start-slave.sh spark://DeepThought:7077
    
    (Using "localhost" instead of "DeepThought" did not work for me)

 3. In my .kiterc:

        export SPARK_MASTER=spark://DeepThought:7077
        export EXECUTOR_MEMORY=6g
        export NUM_EXECUTORS=3
        export NUM_CORES_PER_EXECUTOR=2

## Release guide

Before doing a release, please run the following tests:
```
tools/emr_based_test.sh frontend
test_big_data.sh
```
After this, please create a PR that updates the generated big data test result file.
(`test_big_data.sh` is the same as saying `Big Data Test please` in a PR, but this one can reuse
the cluster started in the previous line.)
