VERSION --use-copy-include-patterns --try 0.6
FROM mambaorg/micromamba:jammy
RUN mkdir /home/mambauser/lk
WORKDIR /home/mambauser/lk
COPY conda-env.* .
RUN ./conda-env.sh build > env.yml && micromamba install -y -n base -f env.yml
# Extra packages for our convenience.
RUN micromamba install -y -n base -c conda-forge vim
USER root
# activate-r-base.sh is slow or hangs.
RUN echo > /opt/conda/etc/conda/activate.d/activate-r-base.sh
# Activate the environment for every command.
COPY earthly/sh /bin/sh
USER mambauser
SAVE IMAGE --cache-hint

sbt-deps:
  # Compile an empty file, just to trigger downloading of the dependencies.
  COPY build.sbt .
  COPY project project
  RUN mkdir dependency-licenses && sbt dumpLicenseReport && cp target/license-reports/lynxkite-licenses.md dependency-licenses/scala.md
  SAVE ARTIFACT dependency-licenses
  SAVE ARTIFACT /home/mambauser/.cache/coursier
  SAVE IMAGE --cache-hint

npm-deps:
  COPY web/package.json web/yarn.lock web/
  RUN cd web && yarn --frozen-lockfile
  RUN mkdir dependency-licenses
  RUN cd web && yarn licenses list | egrep '^└─|^├─|^│  └─|^│  ├─|^   └─|^   ├─' > ../dependency-licenses/javascript.md
  RUN cd web && yarn licenses generate-disclaimer > ../dependency-licenses/javascript.txt
  SAVE ARTIFACT dependency-licenses
  SAVE ARTIFACT web/node_modules
  SAVE IMAGE --cache-hint

grpc:
  COPY sphynx/go.mod sphynx/go.sum sphynx/proto_compile.sh sphynx/sphynx_common.sh sphynx/
  COPY sphynx/proto/*.proto sphynx/proto/
  RUN mkdir app && sphynx/proto_compile.sh
  SAVE ARTIFACT app/com/lynxanalytics/biggraph/graph_api/proto

sphynx-build:
  FROM +grpc
  # Download dependencies.
  RUN cd sphynx && go mod download
  COPY sphynx sphynx
  RUN sphynx/build.sh
  SAVE ARTIFACT sphynx/.build/lynxkite-sphynx
  SAVE ARTIFACT sphynx/.build/zip/lynxkite-sphynx.zip
  SAVE IMAGE --cache-hint

web-build:
  COPY +npm-deps/node_modules web/node_modules
  COPY web web
  COPY .eslintrc.yaml .
  COPY tools/gen_templates.py tools/
  COPY conf/kiterc_template conf/
  RUN cd web && npx gulp
  SAVE ARTIFACT web/dist
  SAVE IMAGE --cache-hint

app-build:
  FROM +sbt-deps
  COPY +grpc/proto app/com/lynxanalytics/biggraph/graph_api/proto
  COPY conf conf
  COPY app app
  COPY resources resources
  RUN sbt compile
  SAVE IMAGE --cache-hint

backend-test-spark:
  FROM +app-build
  COPY test test
  COPY test_backend.sh .
  RUN ./test_backend.sh
  SAVE IMAGE --cache-hint

backend-test-docker:
  FROM +sbt-deps
  USER root
  DO github.com/earthly/lib+INSTALL_DIND
  USER mambauser
  COPY +grpc/proto app/com/lynxanalytics/biggraph/graph_api/proto
  COPY conf conf
  COPY app app
  COPY built-ins built-ins
  COPY test test
  RUN sbt compile
  WITH DOCKER
    RUN sbt 'testOnly -- -n RequiresDocker'
  END

backend-test-sphynx:
  FROM +app-build
  COPY .scalafmt.conf .
  COPY tools/wait_for_port.sh tools/
  COPY test test
  COPY test_backend.sh .
  COPY sphynx/python sphynx/python
  COPY sphynx/r sphynx/r
  COPY +sphynx-build/lynxkite-sphynx sphynx/.build/lynxkite-sphynx
  RUN ./test_backend.sh -s
  SAVE IMAGE --cache-hint

assembly:
  FROM +app-build
  COPY +web-build/dist web/dist
  COPY +sphynx-build/lynxkite-sphynx.zip sphynx/.build/zip/lynxkite-sphynx.zip
  RUN sbt assembly
  RUN mv target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar lynxkite.jar
  SAVE ARTIFACT lynxkite.jar AS LOCAL lynxkite.jar

bash:
  FROM +assembly
  COPY test test
  RUN --interactive bash

python-test:
  COPY tools/wait_for_port.sh tools/
  COPY tools/with_lk.sh tools/
  COPY +assembly/lynxkite.jar target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar
  COPY python python
  COPY conf/kiterc_template conf/
  COPY test/localhost.self-signed.cert* test/
  RUN tools/with_lk.sh python/remote_api/test.sh
  SAVE IMAGE --cache-hint

frontend-test:
  USER root
  RUN apt-get update && apt-get install -y chromium-browser
  USER mambauser
  COPY python python
  COPY web/package.json web/yarn.lock web/
  COPY +assembly/lynxkite.jar target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar
  COPY +npm-deps/node_modules web/node_modules
  USER root
  # Playwright's "install-deps" wants to run "su" which asks for a password.
  # Instead we're already root here, so we want to just run the command.
  RUN cd web && npx playwright install-deps chromium --dry-run 2>/dev/null | sed -n 's/su root/bash/p' | bash
  USER mambauser
  RUN cd web && npx playwright install
  COPY tools/wait_for_port.sh tools/
  COPY tools/with_lk.sh tools/
  COPY web web
  COPY conf/kiterc_template conf/
  COPY test/localhost.self-signed.cert* test/
  ENV CI true
  RUN cd web && ../tools/with_lk.sh yarn playwright test || touch failed
  RUN cd web && zip -qr results.zip playwright-report
  TRY
    RUN [ ! -f web/failed ]
  FINALLY
    SAVE ARTIFACT web/results.zip AS LOCAL results.zip
  END

docker:
  FROM mambaorg/micromamba:jammy
  COPY conda-env.* .
  RUN ./conda-env.sh > env.yml && micromamba install -y -n base -f env.yml
  COPY +assembly/lynxkite.jar .
  COPY conf/kiterc_template .
  CMD ["bash", "-c", ". kiterc_template && spark-submit lynxkite.jar"]
  ENV KITE_ALLOW_PYTHON yes
  ENV KITE_ALLOW_R yes
  ENV KITE_META_DIR /meta
  ENV KITE_DATA_DIR file:/data
  SAVE IMAGE lk

run:
  LOCALLY
  WITH DOCKER --load=+docker
    RUN docker run --rm -v $HOME/kite/meta:/meta -v $HOME/kite/data:/data --name lynxkite-dev -p2200:2200 lk
  END

all-ci:
  # Build this target with --push to update the Earthly cache.
  BUILD +backend-test-spark
  BUILD +backend-test-sphynx
  BUILD +python-test
  # TODO: Re-enable frontend tests.
  # BUILD +frontend-test
