module github.com/c12s/blackhole

replace github.com/coreos/go-systemd/journal => ../../coreos/go-systemd/journal

go 1.13

require (
	github.com/c12s/scheme v0.0.0-20191220185727-c05435488ad4
	github.com/c12s/stellar-go v0.0.0-20191220161710-a82c2c7bb52e
	github.com/coreos/etcd v3.3.18+incompatible
	github.com/coreos/go-systemd/journal v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.1.1 // indirect
	go.uber.org/zap v1.13.0 // indirect
	golang.org/x/net v0.0.0-20191209160850-c0dbc17a3553
	google.golang.org/grpc v1.25.1
	gopkg.in/yaml.v2 v2.2.7
)
