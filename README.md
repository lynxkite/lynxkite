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

Before running the above command you may also need to install the following packages:

    sudo apt-get install libmysqlclient-dev
    sudo apt-get install python3-dev

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

If you want to measure the effect of a code change on big data, run `make big-data-test`
and create a new PR containing the new test results. These results are in `ecosystem/tests/results`.
There are four different data sets used by the performance tests labeled `small`, `medium`, `large` and 
`xlarge`. By default big data tests use the `medium` sized data set.

The tests run on an EMR cluster, launched by `test_big_data.py`. The command 

        make big-data-test
         
builds LynxKite and Ecosystem, and by default uses this new build to run the tests. After building
LynxKite it calls `test_big_data.py`. You can also call `test_big_data.py` manually, but before doing this, 
don't forget to build your currently checked out branch (`make ecosystem` or `ecosystem/native/build.sh`),
or use a release. In the later case you need to update your `biggraph_releases` repo.
     
The performance tests are implemented as Luigi tasks and they use the Python Remote API to
run LynxKite operations on the test data. There are two wrapper tasks `AllTests` and `DefaultTests` which 
 can be used to run predefined test sets. You can also pick a single test task to run.
 
You can find detailed examples in `test_big_data.py` about how to specify parameters of this script for
 fine tuning the performance tests. Here are some examples:
 
        ./test_big_data.py --task AllTests
        ./test_big_data.py --lynx_version native-1.10.0 --test_set_size large --task ModularClustering
        ./test_big_data.py --emr_instance_count 8 --test_size xlarge --task DefaultTests

## Test results on Jenkins

To see the details of the automatic tests, click on `Details` link on GitHub in the box that is
showing the tests.

Jenkins runs on the local network in the Budapest office. To access these results from
outside of the office, you can use the
[SSH gateway](https://github.com/biggraph/deployments/tree/master/budapest-office) and
[FoxyProxy](https://github.com/biggraph/biggraph/wiki/Accessing-our-instances-in-public-clouds).

### Jenkins cleanup

It can happen that Jenkins runs out of _inodes_, and it causes  
"No space left on device" error.  To resolve this issue you can do the following.

 1. Login to Jenkins: `ssh jenkins@192.168.0.37`. (Password is in the secrets repo.)
 
 2. Check available inodes: `df -i`
 
 3. Start docker shell: `docker exec -u root -it lynx-jenkins bash`
 
 4. Delete content of `/tmp` in the container: `rm -Rf /tmp/*`

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
make big-data-test
```
If you are changing Spark settings or Spark version, then also this one:
```
test_spark.sh
```
After this, please create a PR that updates the generated big data test result file.
(`make big-data-test` is the same as saying `Big Data Test please` in a PR, but this one can reuse
the cluster started in the previous line.)
