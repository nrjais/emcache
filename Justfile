start:
	go run cmd/server/main.go

watch:
	watchexec -r -w ./pkg -w ./internal -w ./cmd -w ./migrations -w ./config.yaml "just start"

generate:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		pkg/protos/emcache.proto
	go generate ./...
