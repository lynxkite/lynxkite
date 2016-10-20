find = git ls-files --others --exclude-standard --cached

all: backend

.build/bower-done: web/bower.json web/.bowerrc
	cd web && bower install --silent --config.interactive=false && touch ../$@
.build/node-done: web/package.json
	cd web && npm install --silent
.build/gulp-done: $(shell $(find) web/app) web/gulpfile.js .build/node-done .build/bower-done
	gulp --cwd web
.build/documentation-verified: $(shell $(find) app web) .build/gulp-done
	./tools/check_documentation.sh && touch $@

.build/backend-done: $(shell $(find) app project tools lib conf) build.sbt .build/gulp-done
	./.stage.sh && touch $@
.build/backend-test-passed: $(shell $(find) app test project) build.sbt
	./.test_backend.sh && touch $@
.build/frontend-test-passed: $(shell $(find) web/test) build.sbt .build/backend-done \
                          .build/documentation-verified
	./.test_frontend.sh && touch $@

# Short alias:
.PHONY: all backend frontend backend-test frontend-test test documentation-verified
backend: .build/backend-done
frontend: .build/gulp-done
backend-test: .build/backend-test-passed
frontend-test: .build/frontend-test-passed
test: backend-test frontend-test
documentation-verified: .build/documentation-verified
