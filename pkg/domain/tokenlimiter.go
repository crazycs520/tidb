package domain

import "sync"

// Token is used as a permission to keep on running.
type Token struct{}

// TokenLimiterByKey is used to limit the number of concurrent tasks with different key. Tasks of the same key cannot be concurrent.
type TokenLimiterByKey struct {
	count uint
	ch    chan *Token
	mu    struct {
		sync.Mutex
		keys map[uint64]chan struct{}
	}
}

// Put releases the token.
func (tl *TokenLimiterByKey) Put(tk *Token) {
	tl.ch <- tk
}

// Get obtains a token.
func (tl *TokenLimiterByKey) Gey() *Token {
	return <-tl.ch
}

func (tl *TokenLimiterByKey) AddKey(key uint64) chan struct{} {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	taskCh := tl.mu.keys[key]
	if taskCh == nil {
		tl.mu.keys[key] = make(chan struct{})
	}
	return taskCh
}

func (tl *TokenLimiterByKey) AddKeyOrWaitFinish(key uint64) bool {
	doneCh := tl.AddKey(key)
	if doneCh == nil {
		return true
	}
	_ = <-doneCh
	return false
}

func (tl *TokenLimiterByKey) DeleteKey(key uint64) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	ch := tl.mu.keys[key]
	delete(tl.mu.keys, key)
	if ch != nil {
		close(ch)
	}
}

// NewTokenLimiterByKey creates a TokenLimiter with count tokens.
func NewTokenLimiterByKey(count uint) *TokenLimiterByKey {
	tl := &TokenLimiterByKey{count: count, ch: make(chan *Token, count)}
	tl.mu.keys = make(map[uint64]chan struct{})
	for i := uint(0); i < count; i++ {
		tl.ch <- &Token{}
	}

	return tl
}

// KeyLimiter is used to limit the tasks which has same key to cannot be executed concurrently.
type KeyLimiter struct {
	sync.Mutex
	keys map[uint64]chan struct{}
}

func NewKeyLimiter() *KeyLimiter {
	kl := &KeyLimiter{
		keys: make(map[uint64]chan struct{}),
	}
	return kl
}

func (kl *KeyLimiter) AddKey(key uint64) chan struct{} {
	kl.Lock()
	defer kl.Unlock()
	ch := kl.keys[key]
	if ch == nil {
		kl.keys[key] = make(chan struct{})
	}
	return ch
}

func (kl *KeyLimiter) AddKeyOrWaitFinish(key uint64) bool {
	doneCh := kl.AddKey(key)
	if doneCh == nil {
		return true
	}
	_ = <-doneCh
	return false
}

func (kl *KeyLimiter) ReleaseKey(key uint64) {
	kl.Lock()
	defer kl.Unlock()

	ch := kl.keys[key]
	delete(kl.keys, key)
	if ch != nil {
		close(ch)
	}
}
