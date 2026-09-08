GO ?= go
PYTHON ?= python3
REPO_TOOLS ?= repo-tools
COMPOSE ?= docker compose -f docker/compose.yaml
MODULES := casstore ctlproto manifeststore blobgw blobgw-edge blobgw-s3 mlfs mlfs-csi

.PHONY: build test test-python vet fmt fmt-check check versions ci dev-up dev-down
build:
	@for m in $(MODULES); do (cd $$m && $(GO) build ./...) || exit 1; done
vet:
	@for m in $(MODULES); do (cd $$m && $(GO) vet ./...) || exit 1; done
test:
	@for m in $(MODULES); do (cd $$m && $(GO) test -race -count=1 -timeout=10m ./...) || exit 1; done
test-python:
	cd clients/python && $(PYTHON) -m pytest -q
fmt:
	gofmt -w $(MODULES)
fmt-check:
	@test -z "$$(gofmt -l $(MODULES))" || { gofmt -l $(MODULES); exit 1; }
versions:
	$(REPO_TOOLS) sync-versions
ci:
	$(REPO_TOOLS) generate-ci-gha --force
check: fmt-check vet build test test-python
	$(REPO_TOOLS) sync-versions --check
	$(REPO_TOOLS) generate-ci-gha --check
dev-up:
	$(COMPOSE) up -d --build
dev-down:
	$(COMPOSE) down
