package gocask

import (
	"testing"
)

func Test_encodeDecodeHeader(t *testing.T) {
	tests := []struct {
		timestamp uint32
		keySize   uint32
		valueSize uint32
	}{
		{10, 10, 10},
		{0, 0, 0},
	}
	for _, tt := range tests {
		data := encodeHeader(tt.timestamp, tt.keySize, tt.valueSize)
		timestamp, keySize, valueSize := decodeHeader(data)

		if timestamp != tt.timestamp {
			t.Errorf("encodeHeader() timestamp = %v, want %v", timestamp, tt.timestamp)
		}
		if keySize != tt.keySize {
			t.Errorf("encodeHeader() keySize = %v, want %v", keySize, tt.keySize)
		}
		if valueSize != tt.valueSize {
			t.Errorf("encodeHeader() valueSize = %v, want %v", valueSize, tt.valueSize)
		}
	}
}

func Test_encodeDecodeFileRecord(t *testing.T) {
	tests := []struct {
		timestamp uint32
		key       string
		value     string
		size      int
	}{
		{10, "hello", "world", headerSize + 10},
		{0, "", "", headerSize},
	}
	for _, tt := range tests {
		size, data := encodeFileRecord(tt.timestamp, tt.key, tt.value)
		timestamp, key, value := decodeFileRecord(data)
		if timestamp != tt.timestamp {
			t.Errorf("encodeFileRecord() timestamp = %v, want %v", timestamp, tt.timestamp)
		}
		if key != tt.key {
			t.Errorf("encodeFileRecord() key = %v, want %v", key, tt.key)
		}
		if value != tt.value {
			t.Errorf("encodeFileRecord() value = %v, want %v", value, tt.value)
		}
		if size != tt.size {
			t.Errorf("encodeFileRecord() size = %v, want %v", size, tt.size)
		}
	}
}
