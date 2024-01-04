package main

import (
	"fmt"
	"testing"
)

func TestStorage(t *testing.T) {
	m := NewMemoryStorage()

	for i := 0; i < 100; i++ {
		offset, err := m.Push([]byte(fmt.Sprintf("The new Message is %d", i)))
		if err != nil {
			t.Error(err)
		}

		data, err := m.Fetch(offset)
		if err != nil {
			t.Error(err)
		}
		fmt.Println(string(data))
	}
}
