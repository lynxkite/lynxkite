# How to run this example?
#
# 1. Remove the ecosystem/native/test-mount folder
#
# 2. Build a new ecosystem-docker-release
#
# 3. Start docker release
#    `run_docker_release_test.sh
#
# 4. Run the below code in a Python notebook in JupytyerLab

import lynx.kite
lk = lynx.kite.LynxKite()
print(lk.createExampleGraph().sql('select * from vertices').df())
