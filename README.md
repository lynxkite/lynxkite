LynxKite
========

## Global setup steps

Install `nvm` (https://github.com/creationix/nvm). Then:

    nvm install 5.7
    nvm alias default 5.7
    npm install -g gulp
    # Install Yarn.
    sudo apt-key adv --fetch-keys http://dl.yarnpkg.com/debian/pubkey.gpg
    echo "deb http://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
    sudo apt-get update && sudo apt-get install yarn


Install `Java SDK` and `sbt` (Scala Build Tool):

    echo "deb http://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-get update
    sudo apt-get install openjdk-8-jdk
    sudo apt-get install sbt

(Actually sbt installation would automatically pull a JDK, but in the current
Ubuntu 16.04 LTS for some reason it installs the not-even-released-yet JDK 9. No good.)

Install Spark:

    tools/install_spark.sh

For various tools you will require Python and AWS CLI. To install dependencies please run:

    sudo -H pip3 install -r python_requirements.txt

Spark does a reverse DNS lookup for 0.0.0.0 on startup. At least on Ubuntu 14.04 this is equivalent
to running `avahi-resolve-address 0.0.0.0` and takes 5 seconds. If you want to avoid this delay on
LynxKite startup, add a line to `/etc/avahi/hosts`:

    0.0.0.0 any.local

Install Docker using https://get.docker.com/.

Install Inkscape:

    sudo apt-get install inkscape

Install wkhtmltopdf (0.12.3 or newer).
Just download from http://wkhtmltopdf.org/downloads.html and copy the binary to `/usr/local/bin`.

Install LaTex:

    sudo apt-get install texlive-latex-recommended
    sudo apt-get install texlive-fonts-recommended
    sudo apt-get install texlive-formats-extra

Install `hub`, the command line interface for GitHub.
Download from https://github.com/github/hub/releases and copy the binary to `/usr/local/bin`.

To be able to create signed CloudFront URLs for releases,
obtain `pk-APKAJBDZZHY2ZM7ELY2A.der` and put it in the `$HOME/.ssh` directory.
You can find this file in the secrets repository:
https://github.com/biggraph/secrets
See `README.md` file in that repository on usage.

## Per repository setup

Set up `git` pre-commit hooks:

    tools/git/setup.sh

Set up web build tools:

    cd web
    yarn

## Configure .kiterc

    cp conf/kiterc_template ~/.kiterc
    vi ~/.kiterc # Change defaults if necessary.

## Run LynxKite

To build and run LynxKite invoke the `run.sh` shell script.

## Backend development

Execute `run.sh`. This will build the frontend and the backend if necessary and start Lynxkite
 after.

You can run `make backend-test` to run the backend test, or you can start `sbt` and run
`test-only *SomethingTest` to run just one test.

## Frontend development

When working on the frontend you can avoid running `run.sh` all the time. Start LynxKite with
`run.sh`, then run `gulp.sh` to start a frontend development proxy.  If you access LynxKite through
 the proxy, any changes to frontend files will perform the necessary frontend build steps and reload
 the page in the browser.

Frontend tests (Protractor tests) can be run with `make frotend-test`. This builds and starts
LynxKite, runs the Protractor tests, then shuts down LynxKite. To run a single test the test
code has to be modified. After the one or two `function` parameters add a `'solo'` parameter to mark
 the test for solo running. (Multiple tests can be marked with `'solo'` at the same time.) Run
 `VERBOSE=true gulp test` to enable verbose mode, which prints the tests names as it goes. Run
 `gulp test:serve` to run the tests against the development proxy. This allows for quick iteration
 against tests.

By default LynxKite runs on port 2200 and `gulp test` and `gulp serve` look for it on this port. If
you run LynxKite on a different port (say 1234), run `PORT=1234 gulp test` and `PORT=1234 gulp
test`.

The Protractor tests pop up an actual browser. If you want to avoid this, use `xvfb-run gulp test`.
This will run the frontend tests in a virtual framebuffer. (In Ubuntu `xfvb-run` is available in
package `xvfb`.)

## Ecosystem development

You can run `make ecosystem-test` to run all tests, or run
`{chronomaster,remote_api/python}/test.sh *something*` to run tests from just one file.


## Big Data tests

If you want to measure the effect of a code change on big data, create a PR and add
a comment to it containing the phrase `Big Data Test please`. This will trigger Jenkins
to spin up an EMR cluster, run the tests and push the results as a commit into your PR.

You can manually run these tests using `test_big_data.sh`, and you can also specify more parameters
that way, e.g. `test_big_data.sh pagerank 'fake_westeros_xt_25m'`. For further use cases, see the
comments in `test_big_data.sh`.

## Test results on Jenkins

To see the details of the automatic Jenkins tests, you have to create an ssh tunnel to the
Jenkins machine. For this to work, you need Google Cloud SDK.
The required steps to see the test results:

 1. Install [Google Cloud SDK](https://cloud.google.com/sdk/).

 2. Create ssh tunnel to Jenkins.

        gcloud compute ssh --zone=europe-west1-b jenkins --ssh-flag="-L8888:localhost:80"

 3. Click on `Details` link on GitHub in the box that is showing the tests.


## Run executors on different JVM-s.

Sometimes you want to use a non-local setup on your local machine: e.g., three executors on
three different JVMs, each separate from the driver JVM. This is a better approximation
of a production environment, while still comparatively easy to work with.
Here's how I managed to set it up.

 1. Start spark master:

        ~/spark-<version>/sbin/start-master.sh

 2. Start 3 executors:

        SPARK_WORKER_INSTANCES=3 ~/spark-<version>/sbin/start-slave.sh spark://$(hostname):7077

    (Using "localhost" instead of "$(hostname)" did not work for me)

 3. In my .kiterc:

        export SPARK_MASTER=spark://$(hostname):7077
        export EXECUTOR_MEMORY=6g
        export NUM_EXECUTORS=3
        export NUM_CORES_PER_EXECUTOR=2

## Release guide

Before doing a release, please run the following tests:
```
tools/emr_based_test.sh frontend
test_big_data.sh
```
If you are changing Spark settings or Spark version, then also this one:
```
test_spark.sh
```
After this, please create a PR that updates the generated big data test result file.
(`test_big_data.sh` is the same as saying `Big Data Test please` in a PR, but this one can reuse
the cluster started in the previous line.)
