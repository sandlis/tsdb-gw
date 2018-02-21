package metric_publish

import (
	"sync"
	"time"
)

type Tracker struct {
	Metrics sync.Map
}

func NewTracker() *Tracker {
	t := &Tracker{}
	go t.GC()
	return t
}

func (t *Tracker) GC() {
	ticker := time.NewTicker(time.Hour)
	for range ticker.C {
		t.Metrics.Range(func(k, v interface{}) bool {
			if time.Since(v.(time.Time)) > time.Hour*4 {
				t.Metrics.Delete(k)
			}
			return true
		})
	}
}

func (t *Tracker) Current(key string) bool {
	last, ok := t.Metrics.Load(key)
	return ok && time.Since(last.(time.Time)) < time.Hour*3
}

func (t *Tracker) Store(key string) {
	t.Metrics.Store(key, time.Now())
}
