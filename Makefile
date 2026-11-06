BINARY=flowq

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

.PHONY: build
build:
	go build -o bin/$(BINARY) ./cmd/flowq

.PHONY: chaos
chaos:
	go test -count=1 -tags chaos -run TestChaos -v -timeout 10m ./test/chaos

.PHONY: clean
clean:
	rm -rf bin
