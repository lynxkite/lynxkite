LynxKite
========

## Global setup steps

Install `nvm` (https://github.com/creationix/nvm). Then:

    nvm install v0.10.25
    nvm alias default v0.10.25
    npm install -g grunt-cli bower

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
The recommended solution is to run `run.sh` or `cd web; grunt; cd -` once to build the frontend
once. Then start `sbt` and run the `stage` command whenever you want to rebuild the backend. In
another terminal you can run `stage/bin/biggraph interactive` to start the server after `stage`.

Also in `sbt` you can run `test` to run the full test suite, or `test-only *SomethingTest` to run
just one test.

## Frontend development

When working on the frontend you can also avoid running `run.sh` all the time. Run `grunt.sh` to get
the frontend up with a mock backend. The mock backend serves responses from static files in
`web/testdata`. The advantage of `grunt.sh` is that it watches the frontend files for changes,
automatically rebuilds the frontend if changes are detected, and even reloads the page in the
browser.

Given a running backend, frontend tests can be run with `cd web; grunt test`. To run a single test
the test code has to be modified. After the one or two `function` parameters add a `'solo'`
parameter to mark the test for solo running. (Multiple tests can be marked with `'solo'` at the same
time.) Run `VERBOSE=true grunt test` to enable verbose mode, which prints the tests names as it
goes.

Another useful trick is to use `xvfb-run` (in Ubuntu, it is available in package `xvfb`) as this:
`xvfb-run ./test_frontend.sh`. This will run the frontend tests in a virtual framebuffer.
