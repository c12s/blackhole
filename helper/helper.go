package helper

import (
	"context"
	"errors"
	"google.golang.org/grpc/metadata"
)

func AppendToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "token", token)
}

func ExtractToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("No token in the request")
	}

	if _, ok := md["token"]; !ok {
		return "", errors.New("No token in the request")
	}

	return md["token"][0], nil
}
