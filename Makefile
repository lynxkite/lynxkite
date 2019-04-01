# Can be set from the command line. E.g.:
#   make ecosystem-docker-release VERSION=2.0.0
export VERSION=snapshot
export TEST_SET_SIZE=medium
export EVAL_VERSION_STRING=""

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
	AIRFLOW_GPL_UNIDECODE=yes pip3 install --user -r python_requirements.txt && touch $@
.build/backend-done: \
	$(shell $(find) app project lib conf built-ins) tools/call_spark_submit.sh build.sbt README.md \
	.build/gulp-done licenses
	./tools/install_spark.sh && sbt stage < /dev/null && touch $@
.build/backend-test-passed: $(shell $(find) app test project conf) build.sbt
	./tools/install_spark.sh && ./.test_backend.sh && touch $@
.build/frontend-test-passed: \
		$(shell $(find) web/test) build.sbt .build/backend-done \
		.build/documentation-verified .build/gulp-done
	./.test_frontend.sh && touch $@
.build/remote_api-python-test-passed: $(shell $(find) remote_api/python) .build/backend-done $(pip)
	tools/with_lk.sh remote_api/python/test.sh && touch $@
.build/mobile-prepaid-scv-test-passed: \
	$(shell $(find) remote_api/python mobile-prepaid-scv) .build/backend-done $(pip)
	tools/with_lk.sh mobile-prepaid-scv/unit_test.sh && touch $@
.build/happiness-index-test-mock-data: $(shell $(find) happiness-index/test-data)
	happiness-index/test-data/create_mock_data.sh && touch $@
.build/happiness-index-test-passed: \
	$(shell $(find) remote_api/python happiness-index) .build/backend-done $(pip) \
	.build/happiness-index-test-mock-data
	tools/with_lk.sh happiness-index/unit_test.sh && touch $@
.build/happiness-index-integration-test-passed: \
	.build/happiness-index-test-passed \
	.build/shell_ui-test-passed \
	.build/backend-done $(pip)
	happiness-index/integration-test/test.sh && touch $@
.build/documentation-done-${VERSION}: \
	$(shell $(find) ecosystem/documentation remote_api/python) $(pip)
	ecosystem/documentation/build.sh native && touch $@
.build/ecosystem-done: \
		$(shell $(find) ecosystem/native remote_api ecosystem/docker/base) \
		.build/backend-done .build/documentation-done-${VERSION} $(pip)
	ecosystem/native/tools/build-monitoring.sh && \
	ecosystem/native/bundle.sh && touch $@
.build/ecosystem-docker-base-done: \
		$(shell $(find) ecosystem/docker/base)
	ecosystem/docker/base/build.sh $(VERSION) && touch $@
.build/ecosystem-docker-release-done: \
		.build/ecosystem-done .build/ecosystem-docker-base-done $(shell $(find) ecosystem/docker)
	ecosystem/docker/release/build.sh $(VERSION) && touch $@
.build/shell_ui-test-passed: $(shell $(find) shell_ui) .eslintrc.yaml
	shell_ui/test.sh && touch $@
.build/wide-audience-done: \
		.build/ecosystem-done $(shell $(find) docker/lynxkite/local)
	docker/lynxkite/local/build.sh ${EVAL_VERSION_STRING} && touch $@
scala-dependency-licenses.md: build.sbt
	./tools/install_spark.sh && sbt dumpLicenseReport && cp target/license-reports/biggraph-licenses.md $@
javascript-dependency-licenses.txt: web/package.json
	cd web && LC_ALL=C yarn licenses generate-disclaimer > ../$@
javascript-dependency-licenses.md: web/package.json
	cd web && LC_ALL=C yarn licenses list | egrep '^└─|^├─|^│  └─|^│  ├─|^   └─|^   ├─' > ../$@

# Short aliases for command-line use.
.PHONY: backend
backend: .build/backend-done
.PHONY: frontend
frontend: .build/gulp-done
.PHONY: ecosystem
ecosystem: .build/ecosystem-done
.PHONY: ecosystem-docker-base
ecosystem-docker-base: .build/ecosystem-docker-base-done
.PHONY: ecosystem-docker-release
ecosystem-docker-release: .build/ecosystem-docker-release-done
.PHONY: backend-test
backend-test: .build/backend-test-passed
.PHONY: frontend-test
frontend-test: .build/frontend-test-passed
.PHONY: remote_api-test
remote_api-test: .build/remote_api-python-test-passed
.PHONY: mobile-prepaid-scv-test
mobile-prepaid-scv-test: .build/mobile-prepaid-scv-test-passed
.PHONY: happiness-index-test
happiness-index-test: .build/happiness-index-test-passed
.PHONY: happiness-index-integration-test
happiness-index-integration-test: .build/happiness-index-integration-test-passed
.PHONY: ecosystem-test
ecosystem-test: remote_api-test mobile-prepaid-scv-test happiness-index-test
.PHONY: shell_ui-test
shell_ui-test: .build/shell_ui-test-passed
.PHONY: test
test: backend-test frontend-test ecosystem-test
.PHONY: wide-audience
wide-audience: .build/wide-audience-done
.PHONY: big-data-test
big-data-test: .build/ecosystem-done
	./test_big_data.py --test_set_size ${TEST_SET_SIZE} --rm
.PHONY: licenses
licenses: scala-dependency-licenses.md javascript-dependency-licenses.txt javascript-dependency-licenses.md
