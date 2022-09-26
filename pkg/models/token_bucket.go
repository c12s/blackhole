package models

import (
	"log"
	"time"

	"github.com/google/uuid"
)

type TokenBucket struct {
	ID                 uuid.UUID
	NoOfTokens         int `gorm:"-"`
	MaxTokens          int
	RefreshRate        time.Duration
	LastTokenInsertion time.Time
	NotifyWorker       chan bool `gorm:"-"`
}

func (tb *TokenBucket) TakeToken() bool {
	if tb.NoOfTokens > 0 {
		tb.NoOfTokens -= 1
		go tb.insertToken()
		return true
	}
	return false
}

func (tb *TokenBucket) insertToken() {
	time.Sleep(tb.RefreshRate)
	for time.Now().Sub(tb.LastTokenInsertion) < tb.RefreshRate {
		time.Sleep(tb.RefreshRate)
	}
	tb.LastTokenInsertion = time.Now()
	// This should never happen, checking for errors in system
	if tb.NoOfTokens == tb.MaxTokens {
		log.Printf("[Error] Bucket already filled")
	}
	tb.NoOfTokens += 1
	// tb.NotifyWorker <- true
}
