// Package events stores a bounded, payload-redacted operational event stream.
package events

import (
	"sync"
	"time"
)

type Event struct {
	ID        uint64    `json:"id"`
	Time      time.Time `json:"time"`
	NodeID    string    `json:"nodeId"`
	Type      string    `json:"type"`
	Term      uint64    `json:"term"`
	Index     uint64    `json:"index,omitempty"`
	Peer      string    `json:"peer,omitempty"`
	MessageID string    `json:"messageId,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}
type Buffer struct {
	mu       sync.Mutex
	next     uint64
	items    []Event
	capacity int
}

func New(capacity int) *Buffer {
	if capacity < 1 {
		capacity = 1
	}
	return &Buffer{capacity: capacity}
}
func (b *Buffer) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	e.ID = b.next
	if len(b.items) == b.capacity {
		copy(b.items, b.items[1:])
		b.items = b.items[:len(b.items)-1]
	}
	b.items = append(b.items, e)
}
func (b *Buffer) After(id uint64) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Event, 0)
	for _, e := range b.items {
		if e.ID > id {
			out = append(out, e)
		}
	}
	return out
}
