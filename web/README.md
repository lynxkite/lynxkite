Global setup steps
==================
Install `nvm` (https://github.com/creationix/nvm). Then:

    nvm install v0.10.25
    nvm alias default v0.10.25
    npm install -g grunt-cli bower

Per repository setup
====================
    cd web                        # Basic commands:
    npm install && bower install  # Install dependencies. Run this once.
    npm test                      # Runs tests.
    grunt                         # Lints and builds "dist".
    grunt serve                   # Opens a browser with live reload.
