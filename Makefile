find = git ls-files --others --exclude-standard --cached

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
.build/sphynx-prep-done: $(shell $(find) sphynx)
	sphynx/build.sh && touch $@
.build/backend-done: \
	$(shell $(find) app project lib conf built-ins sphynx) tools/call_spark_submit.sh \
	build.sbt README.md .build/gulp-done .build/licenses-done .build/sphynx-prep-done
	./tools/install_spark.sh && sbt stage < /dev/null && touch $@
.build/backend-test-passed: $(shell $(find) app test project conf) build.sbt \
	.build/sphynx-prep-done
	./tools/install_spark.sh && ./test_backend.sh -s && ./test_backend.sh && touch $@
.build/frontend-test-passed: \
		$(shell $(find) web/test) build.sbt .build/backend-done \
		.build/documentation-verified .build/gulp-done
	./test_frontend.sh && touch $@
.build/remote_api-python-test-passed: $(shell $(find) python/remote_api) .build/backend-done
	tools/with_lk.sh python/remote_api/test.sh && touch $@
dependency-licenses/scala.md: build.sbt
	./tools/install_spark.sh && sbt dumpLicenseReport && cp target/license-reports/lynxkite-licenses.md $@
dependency-licenses/javascript.txt: web/package.json
	cd web && LC_ALL=C yarn licenses generate-disclaimer > ../$@
dependency-licenses/javascript.md: web/package.json
	cd web && LC_ALL=C yarn licenses list | egrep '^└─|^├─|^│  └─|^│  ├─|^   └─|^   ├─' > ../$@
.build/licenses-done: dependency-licenses/scala.md dependency-licenses/javascript.txt dependency-licenses/javascript.md
	touch $@

# Short aliases for command-line use.
.PHONY: backend
backend: .build/backend-done
.PHONY: frontend
frontend: .build/gulp-done
.PHONY: backend-test
backend-test: .build/backend-test-passed
.PHONY: frontend-test
frontend-test: .build/frontend-test-passed
.PHONY: remote_api-test
remote_api-test: .build/remote_api-python-test-passed
.PHONY: test
test: backend-test frontend-test remote_api-test
.PHONY: licenses
licenses: .build/licenses-done
.PHONY: local-bd-test
local-bd-test: .build/backend-done
	python/big_data_tests/run_test.sh
.PHONY: sphynx
sphynx: .build/sphynx-prep-done
