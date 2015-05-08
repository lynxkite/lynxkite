LynxKite
========

Global setup steps
==================
Install `nvm` (https://github.com/creationix/nvm). Then:

    nvm install v0.10.25
    nvm alias default v0.10.25

Install `sbt` (Scala Build Tool):

    echo "deb http://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-get update
    sudo apt-get install sbt

Setup `git`:

    tools/git/setup.sh

Per repository setup
====================
    cd web                          # Basic commands:
    npm install && bower install    # Install dependencies. Run this once.
    npm install -g grunt-cli bower  # Install grunt command line interface. Run this once.
    npm test                        # Runs tests.
    grunt                           # Lints and builds "dist".
    grunt serve                     # Opens a browser with live reload.

Run LynxKite
============
To build and run LynxKite invoke the `run.sh` shell script.
