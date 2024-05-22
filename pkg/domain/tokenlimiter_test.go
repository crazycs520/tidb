package domain

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenLimiterByKeyBasic(t *testing.T) {
	tl := NewTokenLimiterByKey(2)
	oldKey1Ch := tl.AddKey(1)
	require.Nil(t, oldKey1Ch)
	oldKey1Ch = tl.AddKey(1)
	require.NotNil(t, oldKey1Ch)
	oldKey1Ch = tl.AddKey(1)
	require.NotNil(t, oldKey1Ch)
	oldKey2Ch := tl.AddKey(2)
	require.Nil(t, oldKey2Ch)
	oldKey3Ch := tl.AddKey(3)
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
	tl.DeleteKey(1)
	require.True(t, isChanClosed(oldKey1Ch))
}

func TestTokenLimiterByKey(t *testing.T) {
	tl := NewTokenLimiterByKey(2)

	concurrency := 10
	keyCount := 100

	totalCount := int64(0)
	execTask := func(n int) {
		k := uint64(n % keyCount)
		tl.AddKeyOrWaitFinish(k)
		token := tl.Gey()
		time.Sleep(time.Second)
		atomic.AddInt64(&totalCount, 1)
		tl.Put(token)
		tl.DeleteKey(k)
	}
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			execTask(rand.Int())
			//cnt := 0
			//for {
			//	cnt++
			//	select {
			//	case <-time.After(time.Second):
			//		return
			//	default:
			//		execTask(cnt)
			//	}
			//}
		}()
	}
	wg.Wait()
	require.Less(t, totalCount, int64(1))
}
