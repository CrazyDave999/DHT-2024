package main

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
)

const ChunkSize int = 4 * 4096

// filePath -> hashStr1 $ hashStr2 $ ... $ hashStrn
// hashStr -> chunkString
func hashFileChunks(filePath string) (hashStrings []string, chunkStrings []string, size int, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, 0, err
	}
	defer file.Close()

	size = 0
	buffer := make([]byte, ChunkSize)
	for {
		readNum, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, nil, 0, err
		}
		if readNum == 0 {
			break
		}
		size += readNum
		chunk := buffer[:readNum]
		hash := sha1.Sum(chunk)
		hashStr := hex.EncodeToString(hash[:])
		chunkStr := hex.EncodeToString(chunk)
		hashStrings = append(hashStrings, hashStr)
		chunkStrings = append(chunkStrings, chunkStr)
	}
	err = nil
	return
}
