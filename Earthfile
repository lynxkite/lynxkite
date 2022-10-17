VERSION --use-copy-include-patterns 0.6
FROM mambaorg/micromamba:jammy
RUN mkdir /home/mambauser/lk
WORKDIR /home/mambauser/lk
COPY conda-env.yml .
RUN micromamba install -y -n base -f conda-env.yml
# Extra packages for our convenience.
RUN micromamba install -y -n base -c conda-forge vim
# Activate the environment for every command.
USER root
# activate-r-base.sh is slow or hangs.
RUN echo > /opt/conda/etc/conda/activate.d/activate-r-base.sh
COPY earthly/sh /bin/sh
USER mambauser

sbt-deps:
  # Compile an empty file, just to trigger downloading of the dependencies.
  COPY build.sbt .
  COPY project project
  RUN mkdir dependency-licenses && sbt dumpLicenseReport && cp target/license-reports/lynxkite-licenses.md dependency-licenses/scala.md
  SAVE ARTIFACT dependency-licenses
  SAVE ARTIFACT /home/mambauser/.cache/coursier

npm-deps:
  COPY web/package.json web/yarn.lock web/
  RUN cd web; yarn --frozen-lockfile
  RUN mkdir dependency-licenses
  RUN cd web; yarn licenses list | egrep '^└─|^├─|^│  └─|^│  ├─|^   └─|^   ├─' > ../dependency-licenses/javascript.md
  RUN cd web; yarn licenses generate-disclaimer > ../dependency-licenses/javascript.txt
  SAVE ARTIFACT dependency-licenses
  SAVE ARTIFACT web/node_modules

sphynx-build:
  COPY sphynx/go.mod sphynx/go.sum sphynx/proto_compile.sh sphynx/sphynx_common.sh sphynx/
  COPY sphynx/proto/*.proto sphynx/proto/
  RUN mkdir app; sphynx/proto_compile.sh
  # Download dependencies.
  RUN cd sphynx; go mod download
  COPY sphynx sphynx
  RUN sphynx/build.sh
  SAVE ARTIFACT sphynx/.build/lynxkite-sphynx
  SAVE ARTIFACT sphynx/.build/zip/lynxkite-sphynx.zip

web-build:
  COPY +npm-deps/node_modules web/node_modules
  COPY web web
  COPY .eslintrc.yaml .
  COPY tools/gen_templates.py tools/
  COPY conf/kiterc_template conf/
  RUN cd web; npx gulp
  SAVE ARTIFACT web/dist

app-build:
  FROM +sbt-deps
  COPY build.sbt .
  COPY project project
  COPY conf conf
  COPY app app
  RUN sbt compile

backend-test-spark:
  FROM +app-build
  COPY .scalafmt.conf .
  COPY test test
  COPY test_backend.sh .
  RUN ./test_backend.sh

backend-test-sphynx:
  FROM +app-build
  COPY .scalafmt.conf .
  COPY tools/wait_for_port.sh tools/
  COPY test test
  COPY test_backend.sh .
  COPY +sphynx-build/lynxkite-sphynx sphynx/.build/lynxkite-sphynx
  RUN ./test_backend.sh -s

assembly:
  FROM +app-build
  COPY +web-build/dist web/dist
  COPY +sphynx-build/lynxkite-sphynx.zip sphynx/.build/zip/lynxkite-sphynx.zip
  RUN sbt assembly
  RUN mv target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar lynxkite.jar
  SAVE ARTIFACT lynxkite.jar

python-test:
  COPY tools/wait_for_port.sh tools/
  COPY tools/with_lk.sh tools/
  COPY +assembly/lynxkite.jar target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar
  COPY python python
  COPY conf/kiterc_template conf/
  COPY test/localhost.self-signed.cert* test/
  RUN tools/with_lk.sh python/remote_api/test.sh

frontend-test:
  USER root
  RUN apt-get update && apt-get install -y xvfb
  RUN apt-get update && apt-get install -y chromium-browser
  USER mambauser
  COPY tools/wait_for_port.sh tools/
  COPY tools/with_lk.sh tools/
  COPY tools/e2e_test.sh tools/
  COPY +assembly/lynxkite.jar target/scala-2.12/lynxkite-0.1-SNAPSHOT.jar
  COPY +npm-deps/node_modules web/node_modules
  COPY web web
  COPY conf/kiterc_template conf/
  COPY test/localhost.self-signed.cert* test/
  RUN xvfb-run -a tools/with_lk.sh tools/e2e_test.sh


docker:
  FROM mambaorg/micromamba:jammy
  COPY tools/runtime-env.yml .
  RUN micromamba install -y -n base -f runtime-env.yml
  COPY +assembly/lynxkite.jar .
  COPY conf/kiterc_template .
  CMD ["bash", "-c", ". kiterc_template; spark-submit lynxkite.jar"]
  SAVE IMAGE lk

run:
  LOCALLY
  WITH DOCKER --load=+docker
    RUN docker run --rm -p2200:2200 lk
  END
