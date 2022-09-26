package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_TakeToken(t *testing.T) {
	t.Run("When there are no tokens, return false", func(t *testing.T) {
		tb := &TokenBucket{
			NoOfTokens: 0,
		}
		assert.False(t, tb.TakeToken())
	})

	t.Run("When there are tokens, take them", func(t *testing.T) {
		tb := &TokenBucket{
			NoOfTokens: 1,
		}
		assert.True(t, tb.TakeToken())
		assert.Equal(t, 0, tb.NoOfTokens)
	})

	t.Run("Insert new token based on refresh rate", func(t *testing.T) {
		tb := &TokenBucket{
			NoOfTokens:         1,
			MaxTokens:          1,
			RefreshRate:        100 * time.Millisecond,
			LastTokenInsertion: time.Now(),
		}
		assert.True(t, tb.TakeToken())
		assert.Equal(t, 0, tb.NoOfTokens)
		time.Sleep(110 * time.Millisecond)
		assert.Equal(t, 1, tb.NoOfTokens)
	})

	t.Run("When two tokens are taken at the same time", func(t *testing.T) {
		tb := &TokenBucket{
			NoOfTokens:         2,
			MaxTokens:          2,
			RefreshRate:        200 * time.Millisecond,
			LastTokenInsertion: time.Now(),
		}
		tb.TakeToken()
		println("izmedju prva 2 %s", time.Now().String())
		tb.TakeToken()
		assert.Equal(t, 0, tb.NoOfTokens)
		println("pre prvog sleepa 2 %s", time.Now().String())
		time.Sleep(250 * time.Millisecond)
		assert.Equal(t, 1, tb.NoOfTokens)
		println("pre drugog sleepa 2 %s", time.Now().String())
		time.Sleep(250 * time.Millisecond)
		assert.Equal(t, 2, tb.NoOfTokens)
	})

	t.Run("Test worker notification", func(t *testing.T) {
		tb := &TokenBucket{
			NoOfTokens:         1,
			MaxTokens:          1,
			RefreshRate:        100 * time.Millisecond,
			LastTokenInsertion: time.Now(),
			NotifyWorker:       make(chan bool),
		}
		time.Sleep(tb.RefreshRate)
		workers_notified := 0
		// Starting 2 workors, but we expect only one to be notified
		for i := 0; i < 2; i++ {
			go func() {
				<-tb.NotifyWorker
				workers_notified++
			}()
		}
		tb.TakeToken()
		time.Sleep(110 * time.Millisecond)
		assert.Equal(t, 1, workers_notified)
	})
}
