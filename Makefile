find = git ls-files --others --exclude-standard --cached
pip = .build/pip3-packages-installed

all: backend

.build/bower-done: web/bower.json web/.bowerrc
	cd web && bower install --silent --config.interactive=false && touch ../$@
.build/node-done: web/package.json
	cd web && npm install --silent && touch ../$@
.build/gulp-done: $(shell $(find) web/app) web/gulpfile.js .build/node-done .build/bower-done
	gulp --cwd web && touch $@
.build/documentation-verified: $(shell $(find) app) .build/gulp-done
	./tools/check_documentation.sh && touch $@
$(pip): python_requirements.txt
	pip3 install -r python_requirements.txt && touch $@
.build/backend-done: $(shell $(find) app project tools lib conf) build.sbt .build/gulp-done
	sbt stage && touch $@
.build/backend-test-passed: $(shell $(find) app test project conf) build.sbt
	./.test_backend.sh && touch $@
.build/frontend-test-passed: $(shell $(find) web/test) build.sbt .build/backend-done \
                          .build/documentation-verified .build/gulp-done
	./.test_frontend.sh && touch $@
.build/chronomaster-test-passed: $(shell $(find) chronomaster) $(pip)
	chronomaster/test.sh && touch $@
.build/remote_api-python-test-passed: $(shell $(find) remote_api/python) .build/backend-done $(pip)
	tools/with_lk.sh remote_api/python/test.sh && touch $@

# Short alias:
.PHONY: all backend frontend backend-test frontend-test test documentation-verified
backend: .build/backend-done
frontend: .build/gulp-done
backend-test: .build/backend-test-passed
frontend-test: .build/frontend-test-passed
chronomaster-test: .build/chronomaster-test-passed
remote_api-test: .build/remote_api-python-test-passed
ecosystem-test: chronomaster-test remote_api-test
test: backend-test frontend-test ecosystem-test
documentation-verified: .build/documentation-verified
