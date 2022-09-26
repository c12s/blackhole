pb.gen:
	protoc --go_out=./proto proto/blackhole.proto 

gen.server_stub:
	protoc --go_out=./proto --go-grpc_out=./proto ./proto/blackhole.proto 	