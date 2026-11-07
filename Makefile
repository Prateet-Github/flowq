BINARY=bin

.PHONY: all
all: fmt vet test race

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./...

.PHONY: race
race:
	go test -race ./...

.PHONY: bench
bench:
	go test -bench=. -benchmem ./...

BINARY_DIR = bin

.PHONY: build
build:
	mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_DIR)/worker ./cmd/worker
	go build -o $(BINARY_DIR)/producer ./cmd/producer

.PHONY: chaos
chaos:
	go test -count=1 -tags chaos -run TestChaos -v -timeout 10m ./test/chaos

.PHONY: clean
clean:
	rm -rf bin
