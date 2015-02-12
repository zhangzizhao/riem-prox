all: install

GOPATH:=$(CURDIR)
export GOPATH

fmt:
	gofmt -l -w -s src/

dep:fmt
	go get github.com/golang/protobuf/{proto,protoc-gen-go}
	go get github.com/samuel/go-zookeeper/zk
	go get gopkg.in/yaml.v2
	go get gopkg.in/fatih/pool.v2

install:dep
	go install server

