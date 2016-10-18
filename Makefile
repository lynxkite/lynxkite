find = $(shell git ls-files --others --exclude-standard --cached $(1))

all: backend

logs/bower-done: web/bower.json web/.bowerrc
	cd web && bower install --silent --config.interactive=false && touch ../$@
web/node_modules: web/package.json
	cd web && npm install --silent
web/dist: $(call find,web/app) web/gulpfile.js web/node_modules logs/bower-done
	gulp --cwd web
logs/documentation-verified: $(call find,app web) web/dist
	./tools/check_documentation.sh && touch $@

logs/backend-done: $(call find,app project tools lib conf) build.sbt web/dist
	./stage.sh && touch $@
logs/backend-test-passed: $(call find, app test project) build.sbt
	./test_backend.sh && touch $@
logs/frontend-test-passed: $(call find,web/test) build.sbt logs/backend-done \
                          logs/documentation-verified
	./test_frontend.sh && touch $@

# Short alias:
.PHONY: all backend frontend backend-test frontend-test test documentation-verified ~kite.pid run
backend: logs/backend-done
frontend: web/dist
backend-test: logs/backend-test-passed
frontend-test: logs/frontend-test-passed
test: backend-test frontend-test
documentation-verified: logs/documentation-verified
