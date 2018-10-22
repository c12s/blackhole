package queue

import (
	cPb "github.com/c12s/scheme/celestial"
	"google.golang.org/grpc"
	"log"
)

func NewCelestialClient(address string) cPb.CelestialServiceClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to start gRPC connection to celestial service: %v", err)
	}

	return cPb.NewCelestialServiceClient(conn)
}
