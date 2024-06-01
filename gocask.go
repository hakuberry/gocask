package gocask

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

const MaxFileSize int64 = 1 << 30


type Entry struct {
	fileId string
	valueSize uint32
	valuePos uint32
	timestamp uint32
}

type GocaskHandle struct {
	// Datafile directory where instance of Gocask will use this
	// directory to track all datafiles
	datafileDir string
	// Current datafile (Read/Write)
	currentFile *os.File
	// 
	writePosition int
	// KeyDir with map of key and it's entry containing file,
	// offset, and size of most recently written entry
	keyDir map[string]Entry
}

/// newEntry - create new entry for key
///
/// @params fileId - file where key was last modified in
/// @params valueSize - size of value associated with key
/// @params valuePos - position of value in file
/// @params timestamp - timestamp
/// @returns Entry
func newEntry(fileId string, valueSize uint32, valuePos uint32, timestamp uint32) Entry {
	return Entry{fileId, valueSize, valuePos, timestamp}
}


/// isFileExists - check if defined file exists
///
/// @params fileName - file name to check if it exists
/// @return bool - true if file exists, false if file does not exist
func isFileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

/// isMaxFileSize - check if file size exceeds maximum threshold
///
/// @params size - size of file
/// @return error
func (gocask *GocaskHandle) isMaxFileSize(size int) error {
	if gocask.currentFile == nil {
		err := gocask.createNewDataFile()
		return err
	}

	stat, _ := gocask.currentFile.Stat()
	nextSize := stat.Size() + int64(size)
	if nextSize > MaxFileSize {
		err := gocask.createNewDataFile()

		return err
	}

	return nil
}

/// createNewDataFile - create new data file
///
/// @return error
func (gocask *GocaskHandle) createNewDataFile() error {
	
	temp, err := createFilenameId(gocask.currentFile.Name())
	activeFile := temp + ".gocask.dat"
	if err != nil {
		return err
	}

	file, err := os.Create(filepath.Join(gocask.datafileDir, activeFile))
	if err != nil {
		return err
	}
	gocask.currentFile = file
	gocask.writePosition = 0

	return nil
}

/// createFilenameId - create file name
///
/// @params filename - file name to use for new file name
/// @return new file name, error
func createFilenameId(filename string) (string, error) {
	if filename == "" {
		return "", errors.New("File name not defined!")
	}
	pattern := regexp.MustCompile(`(\d+)\.gocask`)
	matches := pattern.FindStringSubmatch(filename)

	filenameId, _ := strconv.Atoi(matches[1])

	return strconv.Itoa(filenameId + 1), nil
}

/// Gocask - create new GocaskHandle
///
/// @params fileName - datafile name (new or existing)
/// @return GoCaskHandle, error
func Gocask(fileName string) (*GocaskHandle, error) {
	gocask := &GocaskHandle{keyDir: make(map[string]Entry)}
	// if the file exists already, then we will load the key_dir
	if isFileExists(fileName) {
		gocask.initializeKeyDir(fileName)
	}
	// we open the file in following modes:
	//	os.O_APPEND - says that the writes are append only.
	// 	os.O_RDWR - says we can read and write to the file
	// 	os.O_CREATE - creates the file if it does not exist
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	gocask.currentFile = file
	return gocask, nil
}

/// initializeKeyDir - initialize keyDir by reading the contents of
///					   file record by record
///
/// @params existingFile - datafile to use to initialize keyDir
func (gocask *GocaskHandle) initializeKeyDir(existingFile string) {
	// we will initialise the keyDir by reading the contents of the file, record by
	// record. As we read each record, we will also update our keyDir with the
	// corresponding Entry
	//
	// NOTE: this method is a blocking one, if the DB size is yuge then it will take
	// a lot of time to startup
	file, _ := os.Open(existingFile)
	defer file.Close()
	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		// TODO: handle errors
		if err != nil {
			break
		}
		timestamp, keySize, valueSize := decodeHeader(header)
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		_, err = io.ReadFull(file, key)
		// TODO: handle errors
		if err != nil {
			break
		}
		_, err = io.ReadFull(file, value)
		// TODO: handle errors
		if err != nil {
			break
		}
		totalSize := headerSize + keySize + valueSize
		gocask.keyDir[string(key)] = newEntry(existingFile, totalSize, uint32(gocask.writePosition), timestamp)
		gocask.writePosition += int(totalSize)
		fmt.Printf("loaded key=%s, value=%s\n", key, value)
	}
}

/// get - retrieve the value from disk
///
/// @params key - key
/// @return value
func (gocask *GocaskHandle) get(key string) string {
	// Get retrieves the value from the disk and returns. If the key does not
	// exist then it returns an empty string
	//
	// How get works?
	//	1. Check if there is any Entry record for the key in keyDir
	//	2. Return an empty string if key doesn't exist
	//	3. If it exists, then read Entry.totalSize bytes starting from the
	//     Entry.position from the disk
	//	4. Decode the bytes into valid KV pair and return the value
	//
	keyEntry, ok := gocask.keyDir[key]
	if !ok {
		return ""
	}

	// Open the file associated with the key as Read Only
	// file, err := os.OpenFile(keyEntry.fileId, os.O_RDONLY, 0666)
	// if err != nil {
		// panic(err)
	// }

	// Move the current pointer to the right offset and read data
	gocask.currentFile.Seek(int64(keyEntry.valuePos), 0)
	data := make([]byte, keyEntry.valueSize)
	_, err := io.ReadFull(gocask.currentFile, data)
	if err != nil {
		panic("Read Error!")
	}

	// Decode the file record associated with key
	// Only need the value (ignore timestamp and key)
	_, _, value := decodeFileRecord(data)
	return value
}

/// set - store key and value on disk and update keyDir
/// 
/// @params key - key
/// @params value - value
func (gocask *GocaskHandle) set(key string, value string) {
	// Set stores the key and value on the disk
	//
	// The steps to save a KV to disk is simple:
	// 1. Encode the KV into bytes
	// 2. Write the bytes to disk by appending to the file
	// 3. Update KeyDir with the Entry of this key
	fileId := gocask.currentFile.Name() + ".gocask.dat"
	timestamp := uint32(time.Now().Unix())
	size, data := encodeFileRecord(timestamp, key, value)
	gocask.write(data)
	gocask.keyDir[key] = newEntry(fileId, uint32(size), uint32(gocask.writePosition), timestamp)
	// update last write position, so that next record can be written from this point
	gocask.writePosition += size
}

/// close - close the datafile
///
/// @return bool
func (gocask *GocaskHandle) close() bool {
	// before we close the file, we need to safely write the contents in the buffers
	// to the disk. Check documentation of GocaskHandle.write() to understand
	// following the operations
	// TODO: handle errors
	gocask.currentFile.Sync()
	if err := gocask.currentFile.Close(); err != nil {
		// TODO: log the error
		return false
	}
	return true
}

/// write - write to file
///
/// @params data - file record
func (gocask *GocaskHandle) write(data []byte) {
	// saving stuff to a file reliably is hard!
	// if you would like to explore and learn more, then
	// start from here: https://danluu.com/file-consistency/
	// and read this too: https://lwn.net/Articles/457667/
	if err := gocask.isMaxFileSize(len(data)); err != nil {
		panic(err)
	}

	if _, err := gocask.currentFile.Write(data); err != nil {
		panic(err)
	}
	// calling fsync after every write is important, this assures that our writes
	// are actually persisted to the disk
	if err := gocask.currentFile.Sync(); err != nil {
		panic(err)
	}
}







