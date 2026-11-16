# Common tasks. Python tests need a virtualenv with the worker and registry test extras.
PY ?= python3
TAG ?= dev
REG ?= infergate.local
CLUSTER ?= infergate

.PHONY: build test test-go test-worker test-registry lint images kind-up kind-images e2e bench chaos compose-up render

build:
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/loadgen ./cmd/loadgen

test: test-go test-worker test-registry

test-go:
	go vet ./...
	go test -race ./...

test-worker:
	cd worker && $(PY) -m pytest -q

test-registry:
	cd registry && $(PY) -m pytest -q

lint:
	gofmt -l . | (! grep .)
	helm lint deploy/helm/infergate
	for f in deploy/helm/infergate/ci/*.yaml; do helm lint deploy/helm/infergate -f $$f; done
	terraform fmt -check -recursive deploy/terraform

render:
	helm template infergate deploy/helm/infergate -n infergate

images:
	docker build -t $(REG)/infergate-gateway:$(TAG) .
	docker build -t $(REG)/infergate-worker:$(TAG) worker
	docker build -t $(REG)/infergate-registry:$(TAG) registry

kind-up:
	kind get clusters | grep -qx $(CLUSTER) || kind create cluster --name $(CLUSTER) --config deploy/kind/cluster.yaml

kind-images: images kind-up
	kind load docker-image --name $(CLUSTER) $(REG)/infergate-gateway:$(TAG) $(REG)/infergate-worker:$(TAG) $(REG)/infergate-registry:$(TAG)

e2e: build kind-images
	./scripts/e2e-kind.sh

bench: build
	PY=$(PY) ./scripts/bench-local.sh

chaos: build
	PY=$(PY) ./scripts/chaos-local.sh

compose-up:
	docker compose up --build --scale worker=3
