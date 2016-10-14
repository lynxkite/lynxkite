web/done: $(shell find web/app)
	gulp --cwd web && touch $@
stage/done: $(shell find app lib) build.sbt web/done
	./stage.sh && touch $@
logs/backend-test-passed: $(shell find app lib) build.sbt
	./test_backend.sh && touch $@

# Short alias:
.PHONY: backend-test
backend-test: logs/backend-test-passed
