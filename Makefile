find = git ls-files --others --exclude-standard --cached

all:

logs/bower-done: web/bower.json
	cd web && bower install --silent --config.interactive=false && touch ../$@
web/node_modules: web/package.json
	cd web && npm install --silent
web/dist: $(shell $(find) web/app) web/gulpfile.js web/node_modules logs/bower-done
	gulp --cwd web
logs/documentation-verified: $(shell $(find) app web) web/dist
	./tools/check_documentation.sh && touch $@

logs/backend-done: $(shell $(find) app) build.sbt web/dist
	./stage.sh && touch $@
logs/backend-test-passed: $(shell $(find) test) build.sbt logs/backend-done
	./test_backend.sh && touch $@
logs/frontend-test-passed: $(shell $(find) web/test) build.sbt logs/backend-done \
                          logs/documentation-verified
	./test_frontend.sh && touch $@

# Short alias:
.PHONY: backend-test documentation-verified frontend-test all backend frontend
backend: logs/backend-done
documentation-verified: logs/documentation-verified
frontend: web/dist
backend-test: logs/backend-test-passed
frontend-test: logs/frontend-test-passed
test: backend-test frontend-test
all: backend
