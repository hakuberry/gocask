package gocask

import (
	"os"
	"testing"
)

func TestGocask_get(t *testing.T) {
	store, err := Gocask("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk store: %v", err)
	}
	defer os.Remove("test.db")
	store.set("name", "fred")
	if val := store.get("name"); val != "fred" {
		t.Errorf("get() = %v, want %v", val, "fred")
	}
}

func TestGocask_getInvalid(t *testing.T) {
	store, err := Gocask("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk store: %v", err)
	}
	defer os.Remove("test.db")
	if val := store.get("a key"); val != "" {
		t.Errorf("get() = %v, want %v", val, "")
	}
}

func TestGocask_setWithPersistence(t *testing.T) {
	store, err := Gocask("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk store: %v", err)
	}
	defer os.Remove("test.db")

	tests := map[string]string{
		"how to build a race car": "newey",
		"shurima shuffle": "faker",
	}
	for key, val := range tests {
		store.set(key, val)
		if store.get(key) != val {
			t.Errorf("get() = %v, want %v", store.get(key), val)
		}
	}
	store.close()
	store, err = Gocask("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk store: %v", err)
	}
	for key, val := range tests {
		if store.get(key) != val {
			t.Errorf("get() = %v, want %v", store.get(key), val)
		}
	}
	store.close()
}

func TestGocask_Delete(t *testing.T) {
	store, err := Gocask("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk store: %v", err)
	}
	defer os.Remove("test.db")

	tests := map[string]string{
		"how to build a race car": "newey",
		"shurima shuffle": "faker",
	}
	for key, val := range tests {
		store.set(key, val)
	}
	for key, _ := range tests {
		store.set(key, "")
	}
	store.set("end", "yes")
	store.close()

	store, err = Gocask("test.db")
	if err != nil {
		t.Fatalf("Failed to create disk store: %v", err)
	}
	for key := range tests {
		if store.get(key) != "" {
			t.Errorf("get() = %v, want '' (empty)", store.get(key))
		}
	}
	if store.get("end") != "yes" {
		t.Errorf("get() = %v, want %v", store.get("end"), "yes")
	}
	store.close()
}
