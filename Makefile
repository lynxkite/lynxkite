find = git ls-files --others --exclude-standard --cached

.SUFFIXES: # Disable built-in rules.
.PHONY: all
all: backend

# Remove all ignored files. Deleting the .idea folder messes with IntelliJ, so exclude
# that.
.PHONY: clean
clean:
	git clean -f -X -d --exclude="!.idea/"

.build/frontend-done: $(shell $(find) web/app) web/vite.config.js web/package.json web/.eslintrc.yaml
	cd web && npm i && npm run eslint && npm run build && cd - && touch $@
.build/documentation-verified: $(shell $(find) app) .build/frontend-done
	./tools/check_documentation.sh && touch $@
.build/sphynx-done: $(shell $(find) sphynx)
	sphynx/build.sh && touch $@
.build/backend-done: \
	$(shell $(find) app project lib conf resources sphynx) \
	build.sbt README.md .build/frontend-done .build/licenses-done .build/sphynx-done
	sbt scalafmt assembly < /dev/null && touch $@
.build/backend-test-spark-passed: $(shell $(find) app test project conf) build.sbt \
	.build/sphynx-done
	./test_backend.sh && touch $@
.build/backend-test-sphynx-passed: $(shell $(find) app test project conf) build.sbt \
	.build/sphynx-done
	./test_backend.sh -s && touch $@
.build/frontend-test-passed: \
		$(shell $(find) web/test) build.sbt .build/backend-done \
		.build/documentation-verified .build/frontend-done
	cd web && ../tools/with_lk.sh npx playwright test --trace on && touch $@
.build/remote_api-python-test-passed: $(shell $(find) python/remote_api) .build/backend-done
	tools/with_lk.sh python/remote_api/test.sh && python/remote_api/managed_tests/run.sh && touch $@
dependency-licenses/scala.md: build.sbt
	sbt dumpLicenseReport && cp target/license-reports/lynxkite-licenses.md $@
dependency-licenses/javascript.txt: web/package.json
	cd web && LC_ALL=C python full_licenses.py > ../$@
dependency-licenses/javascript.md: web/package.json
	cd web && npx license-checker --summary > ../$@ && LC_ALL=C npx license-checker --production | grep -vE 'path:|licenseFile:' >> ../$@
.build/licenses-done: dependency-licenses/scala.md dependency-licenses/javascript.txt dependency-licenses/javascript.md
	touch $@

# Short aliases for command-line use.
.PHONY: backend
backend: .build/backend-done
.PHONY: frontend
frontend: .build/frontend-done
.PHONY: backend-test-spark
backend-test-spark: .build/backend-test-spark-passed
.PHONY: backend-test-sphynx
backend-test-sphynx: .build/backend-test-sphynx-passed
.PHONY: backend-test
backend-test: backend-test-spark backend-test-sphynx
.PHONY: frontend-test
frontend-test: .build/frontend-test-passed
.PHONY: remote_api-test
remote_api-test: .build/remote_api-python-test-passed
.PHONY: test
test: backend-test frontend-test remote_api-test
.PHONY: licenses
licenses: .build/licenses-done
.PHONY: sphynx
sphynx: .build/sphynx-done
