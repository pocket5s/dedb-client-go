.PHONY: all test clean mocks setup run cover genproto
GOCMD=go

test:
	@if [ ! -f /go/bin/gotest ]; then \
		echo "installing gotest..."; \
		go get github.com/rakyll/gotest; \
		go install github.com/rakyll/gotest; \
	fi 
	gotest -v .

cover:
	gotest -covermode=count -coverpkg=. -coverprofile=profile.cov ./... fmt
	go tool cover -func=profile.cov
	go tool cover -html=profile.cov -o=coverage.html

clean:
	go clean
	if test -f go.mod; then echo ""; else rm app; fi
	rm -fr events 

genproto:
	@if [ ! -f /go/bin/protoc-gen-go ]; then \
		echo "installing proto compiler..."; \
		go mod download github.com/golang/protobuf; \
		go get google.golang.org/protobuf/encoding/protojson@v1.26.0; \
		go install github.com/golang/protobuf/protoc-gen-go; \
		go get github.com/favadi/protoc-go-inject-tag; \
		go install github.com/favadi/protoc-go-inject-tag; \
	fi
	rm -fr ./api/
	mkdir ./api/

	go get google.golang.org/protobuf/encoding/protojson@v1.26.0
	go install github.com/golang/protobuf/protoc-gen-go
	rm -fr ./*.pg.go
	protoc --go_out=plugins=grpc:. \
      --proto_path=./ \
      dedb.proto
	mv github.com/pocket5s/dedb-client-go/api/* api/
	rm -fr github.com


build:
	@-$(MAKE) -s clean
	#@-$(MAKE) -s genproto
	go build -o build/client cmd/main.go

setup:
	rm -fr mocks/*.go
	if test -f go.mod; then echo "go.mod exists, skipping"; else go mod init; fi
	go get github.com/rakyll/gotest
	go install github.com/rakyll/gotest
	go get github.com/golang/protobuf/protoc-gen-go
	go install github.com/golang/protobuf/protoc-gen-go

