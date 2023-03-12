.PHONY: gen-proto
gen-proto:
	protoc -I . pkg/mlservice/api/proto/inference.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative