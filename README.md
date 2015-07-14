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

Install Python (for various tools) and Ruby (for AWS CLI and Sass). Then:

    sudo gem install sass


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
