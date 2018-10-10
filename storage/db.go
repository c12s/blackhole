package db

import (
	"context"
)

type DB interface {
	Put(ctx context.Context, data string) error
	Get(ctx context.Context, key string) (string, error)
}
