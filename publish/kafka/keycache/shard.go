package keycache

import (
	"sync"
	"time"

	schema "gopkg.in/raintank/schema.v1"
)

// SubKey is the last 15 bytes of a 16 byte Key
// We can track Key-identified metrics with a SubKey because
// we shard by the first byte of the Key.
type SubKey [15]byte

// Shard tracks for each SubKey when it was last seen
// we know the last seen timestamp with ~10 minute precision
// because all SubKey's Duration's are relative to the ref
type Shard struct {
	sync.Mutex
	ref  Ref
	data map[SubKey]Duration
}

// NewShard creates a new shard
func NewShard(ref Ref) Shard {
	return Shard{
		ref:  ref,
		data: make(map[SubKey]Duration),
	}
}

// Touch marks the key as seen and returns whether it was seen before
// callers should assure that t >= ref and t-ref <= 42 hours
func (s *Shard) Touch(key schema.Key, t time.Time) bool {
	var sub SubKey
	copy(sub[:], key[1:])
	s.Lock()
	_, ok := s.data[sub]
	s.data[sub] = NewDuration(s.ref, t)
	s.Unlock()
	return ok
}

// Len returns the length of the shard
func (s *Shard) Len() int {
	s.Lock()
	l := len(s.data)
	s.Unlock()
	return l
}

// Prune removes stale entries from the shard.
// important that we update ref of the shard at least every 42 hours
// so that duration doesn't overflow
func (s *Shard) Prune(now time.Time, diff Duration) int {
	newRef := NewRef(now)
	s.Lock()

	// the amount to subtract of a duration for it to be based on the new reference
	// we know subtract fits into a Duration since we call Prune at least every 42 hours
	subtract := Duration(newRef - s.ref)

	cutoff := newRef - Ref(diff)

	for subkey, duration := range s.data {
		// remove entry if it is too old, e.g. if:
		// newRef - diff > "timestamp of the entry in 10minutely buckets"
		// newRef - diff > ref + duration
		if cutoff > s.ref+Ref(duration) {
			delete(s.data, subkey)
			continue
		}

		// note that the update formula is only correct if these 2 conditions:
		// A) it does not underflow
		// iow: duration - subtract >= 0
		// iow: duration >= subtract          (1)
		// we already know from above:
		// newRef - diff <= ref + duration    (2)
		// we also know that:
		// subtract == newRef - ref.          (3)
		//
		// put (3) into (1):
		// duration >= newRef - ref           (4)
		// put (4) into (2):
		// newRef - diff <= ref + newRef - ref
		// iow: - diff <= 0
		// iow: diff >= 0
		// we know this is true, so there is no underflow.

		// B) the result fits into a uint8. but since we decrease the amount, to a new >= 0 amount,
		// we know it does

		s.data[subkey] = duration - subtract
	}
	s.ref = newRef
	remaining := len(s.data)
	s.Unlock()
	return remaining
}
