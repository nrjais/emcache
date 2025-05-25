start:
	go run cmd/server/main.go

start-ci:
	go run cmd/server/main.go -config config-ci.yaml

watch:
	watchexec -r -w ./pkg -w ./internal -w ./cmd -w ./migrations -w ./config.yaml "just start"

install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install go.uber.org/mock/mockgen@latest

generate: install-tools
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		pkg/protos/emcache.proto
	go generate ./...

test:
	go test ./...

test-unit:
	go test -race -coverprofile=coverage.out -covermode=atomic ./internal/... ./pkg/...

build:
	go build -o emcache cmd/server/main.go

build-ci:
	go build -o emcache-server cmd/server/main.go

docker-build:
	docker build -t emcache .

docker-run: docker-build
	docker run -v $(pwd)/config.yaml:/etc/emcache/config.yaml emcache
