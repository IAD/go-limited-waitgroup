package go_limited_waitgroup

import (
	"sync"
)

type LimitedWaitGroup struct {
	limit   int
	counter chan struct{}
	lock    sync.RWMutex
	doneCh  chan struct{}
	isDone  bool
}

func NewLimitedWaitGroup(limit int) *LimitedWaitGroup {
	return &LimitedWaitGroup{
		limit:   limit,
		doneCh:  make(chan struct{}),
		counter: make(chan struct{}, limit),
	}
}

func (wg *LimitedWaitGroup) Add(delta int) {
	if delta == 0 {
		return
	}
	if delta > wg.limit {
		panic("sync: negative WaitGroup counter")
	}
	if len(wg.counter) == 0 {
		wg.doneCh = make(chan struct{})
		wg.isDone = false
	}

	for i := 0; i < delta; i++ {
		wg.counter <- struct{}{}
	}
}

func (wg *LimitedWaitGroup) Done() {
	wg.lock.Lock()
	defer wg.lock.Unlock()

	if wg.isDone {
		panic("sync: negative WaitGroup counter")
	}

	if len(wg.counter) > 0 {
		<-wg.counter
	}
	if len(wg.counter) == 0 {
		wg.isDone = true
		close(wg.doneCh)
	}
}

func (wg *LimitedWaitGroup) Wait() {
	<-wg.doneCh
}
