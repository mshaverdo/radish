all: deps build test-race

build:
	go build -o $(GOPATH)/bin/radish-server github.com/mshaverdo/radish/cmd/radish-server
	go build -o $(GOPATH)/bin/radish-benchmark github.com/mshaverdo/radish/cmd/radish-benchmark

test:
	go test github.com/mshaverdo/radish/core github.com/mshaverdo/radish/controller/httpserver github.com/mshaverdo/radish/radish-client

test-race:
	go test -race github.com/mshaverdo/radish/core github.com/mshaverdo/radish/controller/httpserver github.com/mshaverdo/radish/radish-client

deps:
	dep ensure