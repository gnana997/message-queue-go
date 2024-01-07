package main

import (
	"fmt"
	"sync"
)

type StorageProducerFunc func() Storage

type Storage interface {
	Push([]byte) (int, error)
	Fetch(int) ([]byte, error)
	Size() int
}

type MemoryStorage struct {
	mu   sync.RWMutex
	data [][]byte
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make([][]byte, 0),
	}
}

func (m *MemoryStorage) Push(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = append(m.data, b)
	return len(m.data) - 1, nil
}

func (m *MemoryStorage) Fetch(offset int) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be less than 0")
	}

	if offset > len(m.data)-1 {
		return nil, fmt.Errorf("offset (%d) is too high", offset)
	}

	return m.data[offset], nil
}

func (m *MemoryStorage) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.data) - 1
}
