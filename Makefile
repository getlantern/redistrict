GOPACKAGES = $(shell go list ./...  | grep -v /vendor/)

default: build

build: 
	go build

test: test-all

test-all:
	@go test -v $(GOPACKAGES)
