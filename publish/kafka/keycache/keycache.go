package keycache

import (
	"sync"
	"time"

	schema "gopkg.in/raintank/schema.v1"
)

type SubKey [15]byte

type KeyCache struct {
	staleThresh   uint8 // number of 10-minutely periods
	pruneInterval time.Duration

	sync.RWMutex
	caches map[uint32]*Cache
}

func NewKeyCache(staleThresh, pruneInterval time.Duration) *KeyCache {
	if staleThresh.Hours() > 40 {
		panic("stale time may not exceed 40 hours due to resolution of internal bookkeeping")
	}
	if pruneInterval.Hours() > 40 {
		panic("prune interval may not exceed 40 hours due to resolution of internal bookkeeping")
	}
	k := &KeyCache{
		pruneInterval: pruneInterval,
		staleThresh:   uint8(staleThresh.Nanoseconds() / 1e9 / 600),
		caches:        make(map[uint32]*Cache),
	}
	go k.prune()
	return k
}

// marks the key as seen and returns whether it was seen before
func (k *KeyCache) Touch(key schema.MKey, t time.Time) bool {
	k.RLock()
	cache, ok := k.caches[key.Org]
	k.RUnlock()
	// most likely this branch won't execute
	if !ok {
		k.Lock()
		// check again in case another routine has just added it
		cache, ok = k.caches[key.Org]
		if !ok {
			cache = NewCache(NewRef(t))
			k.caches[key.Org] = cache
		}
		k.Unlock()
	}
	return cache.Touch(key.Key, t)
}

func (k *KeyCache) Len() int {
	var sum int
	k.RLock()
	caches := make([]*Cache, 0, len(k.caches))
	for _, c := range k.caches {
		caches = append(caches, c)
	}
	k.RUnlock()
	for _, c := range caches {
		sum += c.Len()
	}
	return sum
}

func (k *KeyCache) prune() {
	tick := time.NewTicker(k.pruneInterval)
	for now := range tick.C {

		type target struct {
			org   uint32
			cache *Cache
		}

		k.RLock()
		targets := make([]target, 0, len(k.caches))
		for org, c := range k.caches {
			targets = append(targets, target{
				org,
				c,
			})
		}
		k.RUnlock()

		for _, t := range targets {
			size := t.cache.Prune(now, k.staleThresh)
			if size == 0 {
				k.Lock()
				delete(k.caches, t.org)
				k.Unlock()
			}
		}
	}
}
