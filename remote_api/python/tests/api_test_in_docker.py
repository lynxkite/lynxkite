# How to run this example?
#
# Before starting docker image
#
# 0. Modify `ecosystem/native/test-mount/config/central` for local run:
#    a) KITE_DATA_DIR=file:/lynxkite/KITE_DATA
#    b) SPARK_MASTER=local
#    c) # NUM_EXECUTORS=1
# 1. Stop mysql server if running, before start the container
#    `/etc/init.d/mysql stop`
# 2. Start docker release
#    `run_docker_release_test.sh ../native/test-mount/`
# 3. Open bash in running container
#    `docker exec -it docker_ecosystem_1 bash`
#
# In the docker container
#
# 4. `source /lynxkite/config/central` to set PYTHONPATH
# 5. Use Python3.6 in docker:
#    `/opt/conda/bin/python3.6 /lynxkite/apps/remote_api/python/tests/api_test_in_docker.py`

import lynx.kite
lk = lynx.kite.LynxKite()
print(lk.createExampleGraph().sql('select * from vertices').df())
