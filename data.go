package gocask

import "encoding/binary"


// Header size is defined by the following:
// timestamp (32 bits)
// keySize (32 bits)
// valueSize (32 bits)
const headerSize = 12

/// encodeHeader - encode header to bytes
///
/// @params timestamp - timestamp when record is generated
/// @params keySize - size of key in bytes
/// @params valueSize - size of value in bytes
/// @returns header from timestamp, keySize, valueSize encoded to bytes
func encodeHeader(timestamp uint32, keySize uint32, valueSize uint32) []byte {
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], timestamp)
	binary.LittleEndian.PutUint32(header[4:8], keySize)
	binary.LittleEndian.PutUint32(header[8:12], valueSize)
	return header
}

/// decodeHeader - decode header from bytes to 
///				   timestamp
///				   keySize
///			 	   valueSize
///
/// @params header - header in bytes
/// @returns timestamp, keySize, valueSize
func decodeHeader(header []byte) (uint32, uint32, uint32) {
	timestamp := binary.LittleEndian.Uint32(header[0:4])
	keySize := binary.LittleEndian.Uint32(header[4:8])
	valueSize := binary.LittleEndian.Uint32(header[8:12])
	return timestamp, keySize, valueSize
}

/// encodeFileRecord - encode timestamp, key, value to bytes
///
/// @params timestamp - timestamp when record is generated
/// @params key - key
/// @params value - value
/// @returns total size of record, data of record
func encodeFileRecord(timestamp uint32, key string, value string) (int, []byte) {
	header := encodeHeader(timestamp, uint32(len(key)), uint32(len(value)))
	data := append([]byte(key), []byte(value)...)
	return headerSize + len(data), append(header, data...)
}

/// decodeFileRecord - decode file record from bytes to 
///					   timestamp
///					   key
///					   value
///
/// @params data - file record in bytes
/// @returns timestamp, key, value
func decodeFileRecord(data []byte) (uint32, string, string) {
	timestamp, keySize, valueSize := decodeHeader(data[0:headerSize])
	key := string(data[headerSize : headerSize+keySize])
	value := string(data[headerSize+keySize : headerSize+keySize+valueSize])
	return timestamp, key, value
}
