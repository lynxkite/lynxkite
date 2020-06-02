# Can be set from the command line. E.g.:
#   make ecosystem-docker-release VERSION=2.0.0
export VERSION=snapshot
export TEST_SET_SIZE=medium

find = git ls-files --others --exclude-standard --cached
pip = .build/pip3-packages-installed

.SUFFIXES: # Disable built-in rules.
.PHONY: all
all: backend

# Remove all ignored files. Deleting the .idea folder messes with IntelliJ, so exclude
# that.
.PHONY: clean
clean:
	git clean -f -X -d --exclude="!.idea/"

.build/gulp-done: $(shell $(find) web/app) web/gulpfile.js web/package.json .eslintrc.yaml
	cd web && LC_ALL=C yarn --frozen-lockfile && npx gulp && cd - && touch $@
.build/documentation-verified: $(shell $(find) app) .build/gulp-done
	./tools/check_documentation.sh && touch $@
$(pip): python_requirements.txt
	AIRFLOW_GPL_UNIDECODE=yes pip install --user -r python_requirements.txt && touch $@
.build/sphynx-prep-done: $(shell $(find) sphynx)
	sphynx/build.sh && touch $@
.build/backend-done: \
	$(shell $(find) app project lib conf built-ins sphynx) tools/call_spark_submit.sh \
	build.sbt README.md .build/gulp-done .build/licenses-done .build/sphynx-prep-done
	./tools/install_spark.sh && sbt stage < /dev/null && touch $@
.build/backend-test-passed: $(shell $(find) app test project conf) build.sbt \
	.build/sphynx-prep-done
	./tools/install_spark.sh && ./.test_backend.sh -s && ./.test_backend.sh && touch $@
.build/frontend-test-passed: \
		$(shell $(find) web/test) build.sbt .build/backend-done \
		.build/documentation-verified .build/gulp-done
	./.test_frontend.sh && touch $@
.build/remote_api-python-test-passed: $(shell $(find) python/remote_api) .build/backend-done $(pip)
	tools/with_lk.sh python/remote_api/test.sh && touch $@
.build/automation-python-test-passed: $(shell $(find) python/remote_api python/automation) \
		.build/backend-done $(pip)
	tools/with_lk.sh python/automation/test.sh && touch $@
.build/versioning-installed: \
	python/versioning/lynx_versioning.py
	tools/distribute_lynx_versioning.sh && touch $@
.build/standard-pipelines-test-passed: \
	$(shell $(find) python/remote_api python/automation standard-pipelines) .build/backend-done $(pip)
	tools/with_lk.sh standard-pipelines/unit_test.sh && touch $@
.build/impact-analyzer-pipeline-test-passed: \
		$(shell $(find) standard-pipelines/impact-analyzer) \
		impact-analyzer-dashboard/server/src/configChecker.ts
	standard-pipelines/impact-analysis/unit_test.sh && touch $@
.build/documentation-done-${VERSION}: \
	$(shell $(find) ecosystem/documentation python/remote_api python/automation) $(pip)
	ecosystem/documentation/build.sh native && touch $@
.build/ecosystem-done: \
		$(shell $(find) ecosystem/native python/remote_api python/automation) \
		.build/versioning-installed \
		.build/backend-done .build/documentation-done-${VERSION} $(pip)
	ecosystem/native/tools/build-monitoring.sh && \
	ecosystem/native/bundle.sh $(VERSION) && touch $@
.build/ecosystem-docker-release-done: \
		.build/ecosystem-done \
		$(shell $(find) ecosystem/docker)
	ecosystem/docker/build.sh $(VERSION) && touch $@
.build/shell_ui-test-passed: $(shell $(find) shell_ui) .eslintrc.yaml
	shell_ui/test.sh && touch $@
.build/impact-analyzer-dashboard-test-passed: $(shell $(find) impact-analyzer-dashboard)
	impact-analyzer-dashboard/tests.sh && touch $@
scala-dependency-licenses.md: build.sbt
	./tools/install_spark.sh && sbt dumpLicenseReport && cp target/license-reports/lynxkite-licenses.md $@
javascript-dependency-licenses.txt: web/package.json
	cd web && LC_ALL=C yarn licenses generate-disclaimer > ../$@
javascript-dependency-licenses.md: web/package.json
	cd web && LC_ALL=C yarn licenses list | egrep '^└─|^├─|^│  └─|^│  ├─|^   └─|^   ├─' > ../$@

.build/licenses-done: scala-dependency-licenses.md javascript-dependency-licenses.txt javascript-dependency-licenses.md
	touch $@

# Short aliases for command-line use.
.PHONY: backend
backend: .build/backend-done
.PHONY: frontend
frontend: .build/gulp-done
.PHONY: ecosystem
ecosystem: .build/ecosystem-done
.PHONY: ecosystem-docker-release
ecosystem-docker-release: .build/ecosystem-docker-release-done
.PHONY: backend-test
backend-test: .build/backend-test-passed
.PHONY: frontend-test
frontend-test: .build/frontend-test-passed
.PHONY: remote_api-test
remote_api-test: .build/remote_api-python-test-passed
.PHONY: automation-test
automation-test: .build/automation-python-test-passed
.PHONY: standard-pipelines-test
standard-pipelines-test: .build/standard-pipelines-test-passed
.PHONY: impact-analyzer-pipeline-test
impact-analyzer-pipeline-test: .build/impact-analyzer-pipeline-test-passed
.PHONY: ecosystem-test
ecosystem-test: remote_api-test automation-test standard-pipelines-test
.PHONY: shell_ui-test
shell_ui-test: .build/shell_ui-test-passed
.PHONY: impact-analyzer-dashboard-test
impact-analyzer-dashboard-test: .build/impact-analyzer-dashboard-test-passed
.PHONY: test
test: backend-test frontend-test ecosystem-test
.PHONY: big-data-test
big-data-test: .build/ecosystem-done
	./test_big_data.py --test_set_size ${TEST_SET_SIZE} --rm
.PHONY: licenses
licenses: .build/licenses-done
.PHONY: local-bd-test
local-bd-test: .build/backend-done
	python/big_data_tests/run_test.sh
.PHONY: sphynx
sphynx: .build/sphynx-prep-done
