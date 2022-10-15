VERSION 0.6
FROM mambaorg/micromamba:jammy

conda-deps:
  COPY conda-env.yml .
  RUN micromamba install -y -n base -f conda-env.yml

sbt-deps:
  FROM +conda-deps
  # TODO: Move this earlier.
  USER root
  RUN apt-get update && apt-get install -y vim
  RUN echo > /opt/conda/etc/conda/activate.d/activate-r-base.sh
  COPY earthly/sh /bin/sh
  USER mambauser
  RUN mkdir /home/mambauser/lk
  WORKDIR /home/mambauser/lk

  # Compile an empty file, just to trigger downloading of the dependencies.
  COPY build.sbt .
  COPY project project
  RUN touch a.scala && sbt compile && rm a.scala
  RUN mkdir dependency-licenses && sbt dumpLicenseReport && cp target/license-reports/lynxkite-licenses.md dependency-licenses/scala.md

npm-deps:
  FROM +sbt-deps
  COPY web/package.json web/yarn.lock web/
  RUN cd web; yarn --frozen-lockfile
  RUN cd web; yarn licenses list | egrep '^└─|^├─|^│  └─|^│  ├─|^   └─|^   ├─' > ../dependency-licenses/javascript.md
  RUN cd web; yarn licenses generate-disclaimer > ../dependency-licenses/javascript.txt

sphynx-build:
  FROM +npm-deps
# TODO: Move up.
  USER root
  RUN apt-get update && apt-get install -y wget zip sudo
  RUN adduser mambauser sudo
  USER mambauser

  COPY sphynx/go.mod sphynx/go.sum sphynx/proto_compile.sh sphynx/sphynx_common.sh sphynx/
  COPY sphynx/proto/*.proto sphynx/proto/
  RUN mkdir app; sphynx/proto_compile.sh
  COPY sphynx sphynx
  RUN sphynx/build.sh

web-build:
  FROM +sphynx-build
  COPY web web
  COPY .eslintrc.yaml .
  COPY tools/gen_templates.py tools/
  COPY conf/kiterc_template conf/
  RUN cd web; npx gulp

app-build:
  FROM +web-build
  COPY app app
  RUN sbt assembly
