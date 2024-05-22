package domain

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewKeyLimiterBasic(t *testing.T) {
	kl := NewKeyLimiter()
	oldKey1Ch := kl.AddKey(1)
	require.Nil(t, oldKey1Ch)
	oldKey1Ch = kl.AddKey(1)
	require.NotNil(t, oldKey1Ch)
	oldKey1Ch = kl.AddKey(1)
	require.NotNil(t, oldKey1Ch)
	oldKey2Ch := kl.AddKey(2)
	require.Nil(t, oldKey2Ch)
	oldKey3Ch := kl.AddKey(3)
	require.Nil(t, oldKey3Ch)
	isChanClosed := func(ch chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	require.False(t, isChanClosed(oldKey1Ch))
	require.Equal(t, 3, len(kl.keys))
	kl.ReleaseKey(1)
	require.True(t, isChanClosed(oldKey1Ch))
	require.Equal(t, 2, len(kl.keys))
	// release unknown key is ok.
	kl.ReleaseKey(100)
	require.Equal(t, 2, len(kl.keys))
	kl.ReleaseKey(2)
	kl.ReleaseKey(3)
	require.Equal(t, 0, len(kl.keys))
}

func TestTokenLimiterByKey(t *testing.T) {
	kl := NewKeyLimiter()

	concurrency := 10
	totalCount := int64(0)
	benchFn := func(genKeyFn func() int) {
		atomic.StoreInt64(&totalCount, 0)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		execTask := func(k uint64) {
			kl.AddKeyOrWaitFinish(k)
			defer kl.ReleaseKey(k)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * 80):
				atomic.AddInt64(&totalCount, 1)
			}
		}
		var wg sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				key := genKeyFn()
				execTask(uint64(key))
			}()
		}
		wg.Wait()
	}
	// all concurrency worker run task with same key 1, then the final qps should be 1.
	benchFn(func() int {
		return 1
	})
	require.Equal(t, int64(1), totalCount)

	// all concurrency worker run task with rand key [0, 1, 2], then the final qps should be 3.
	k := 0
	benchFn(func() int {
		k++
		return k % 3
	})
	require.Equal(t, int64(3), totalCount)

	// all concurrency worker run task with different key, then the final qps should same with concurrency.
	k = 0
	benchFn(func() int {
		k++
		return k
	})
	require.Equal(t, int64(concurrency), totalCount)
}
