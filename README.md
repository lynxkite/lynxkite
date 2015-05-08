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

Check out the [README.md](web/README.md) file in the `web` directory to do the per repository setup steps.

Run LynxKite
============
To build and run LynxKite invoke the `run.sh` shell script.
