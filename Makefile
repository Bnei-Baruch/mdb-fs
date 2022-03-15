GO_FILES = $(shell find . -path ./vendor -prune -o -type f -name "*.go" -print)

build: clean test
	@go build

clean:
	rm -f mdb-fs

test:
	go test $(shell go list ./... | grep -v /vendor/)

lint:
	@golint $(GO_FILES) || true

fmt:
	@gofmt -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")

.PHONY: all clean test lint fmt
