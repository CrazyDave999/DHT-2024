package main

import (
	"CDN/Kademlia"
	"bufio"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

type FileMeta struct {
	name string
	size int
}

var files []FileMeta
var nodes [NodeSize + 1]*Kademlia.Node
var nodeAddresses [NodeSize + 1]string

func Init() {
	var wg sync.WaitGroup
	for i := 0; i <= NodeSize; i++ {
		nodes[i] = NewNode(firstPort + i)
		nodeAddresses[i] = portToAddr(localAddress, firstPort+i)
		wg.Add(1)
		go func(i int) {
			nodes[i].Run()
			wg.Done()
		}(i)
	}
	wg.Wait()
	nodes[0].Create()
	for i := 1; i <= NodeSize; i++ {
		nodes[i].Join(nodeAddresses[0])
	}
	time.Sleep(2 * time.Second)
}

func listConfig() {
	cyan.Printf("Configuration:\n")
	fmt.Printf("Chunk size: %vBytes\n", ChunkSize)
	fmt.Printf("Quantity of nodes: %v\nNode list:\n", NodeSize+1)
	for i, ip := range nodeAddresses {
		fmt.Printf("%s    ", ip)
		if i%4 == 3 {
			fmt.Printf("\n")
		}
	}
	fmt.Printf("\n")
}

func listFiles() {
	cyan.Printf("Listing all %v files in CDN network:\n", len(files))
	for i, file := range files {
		fmt.Printf("%v. %s %vBytes\n", i, file.name, file.size)
	}
}

// filePath -> hashStr1 $ hashStr2 $ ... $ hashStrn
// hashStr -> chunkString
func uploadFile(filePath string, name string) {
	cyan.Printf("Now try to upload file: %s with name: %s\n", filePath, name)
	hashStrings, chunkStrings, size, err := hashFileChunks(filePath)
	if err != nil {
		red.Printf("Failed to upload file: %s. %s\n", filePath, err)
		return
	}
	fmt.Printf("Parse file: %s successfully. Generating hash value...\n", filePath)
	hashValue := strings.Join(hashStrings, "$")
	fmt.Printf("Generated hash value: \n%s\n", hashValue)
	curNode := nodes[rand.Intn(len(nodes))]
	curNode.Put(name, hashValue)
	fmt.Printf("There are %v pieces in total. Uploading concurrently...\n", len(hashStrings))
	var wg sync.WaitGroup
	wg.Add(len(hashStrings))
	flag := true
	var lock sync.Mutex
	for i := 0; i < len(hashStrings); i++ {
		go func(i int) {
			index := rand.Intn(len(nodes))
			curNode = nodes[index]
			curIp := nodeAddresses[index]
			ok := curNode.Put(hashStrings[i], chunkStrings[i])
			if ok {
				green.Printf("Uploaded the piece #%v succesfully by node: %s\n", i, curIp)
			} else {
				red.Printf("Uploaded the piece #%v failed from node: %s\n", i, curIp)
				lock.Lock()
				flag = false
				lock.Unlock()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	if !flag {
		red.Printf("Failed to upload file: %s. Some pieces failed to be uploaded.\n", filePath)
		return
	}
	files = append(files, FileMeta{name, size})
	green.Printf("Uploaded file: %s with name: %s successfully\n", filePath, name)
}

func downloadFile(name string, filePath string) {
	cyan.Printf("Now try to download file: %s as: %s\n", name, filePath)
	index := rand.Intn(len(nodes))
	curNode := nodes[index]
	ok, hashValue := curNode.Get(name)
	if !ok {
		red.Printf("Sorry, faild downloading file: %s. Not found.\n", name)
		return
	}
	fmt.Printf("Got hash value: \n%s\n", hashValue)
	hashStrings := strings.Split(hashValue, "$")
	num := len(hashStrings)
	fmt.Printf("There are %v pieces in total. Downloading...\n", num)
	flag := true
	var lock sync.Mutex
	chunks := make([]string, num)

	var wg sync.WaitGroup
	wg.Add(num)
	for i, hash := range hashStrings {
		go func(i int, hash string) {
			index = rand.Intn(len(nodes))
			curNode = nodes[index]
			curIp := nodeAddresses[index]
			ok, value := curNode.Get(hash)
			lock.Lock()
			if ok {
				chunks[i] = value
				green.Printf("Downloaded the piece #%v succesfully from node: %s\n", i, curIp)
			} else {
				red.Printf("Downloaded the piece #%v failed from node: %s\n", i, curIp)
				flag = false
			}
			lock.Unlock()
			wg.Done()
		}(i, hash)
	}
	wg.Wait()
	if !flag {
		red.Printf("Sorry, faild downloading file: %s. Some pieces missed.\n", name)
		return
	}
	fmt.Printf("Successfully collected all pieces. Combining and writing...\n")
	file, err := os.Create(filePath)
	if err != nil {
		red.Printf("Sorry, failed to create file: %s\n", filePath)
		return
	}
	defer file.Close()
	for i := 0; i < num; i++ {
		chunkBytes, err := hex.DecodeString(chunks[i])
		if err != nil {
		}
		_, err = file.Write(chunkBytes)
		if err != nil {
		}
	}
	green.Printf("Downloaded successfully. File has been written to %s\n", filePath)
}

func main() {
	Init()
	reader := bufio.NewReader(os.Stdin)
	cyan.Println("Welcome to CrazyDave's DHT application!")
	for {
		yellow.Print("$ ")
		input, err := reader.ReadString('\n')
		if err != nil {
			red.Println("Invalid input: ", err)
			continue
		}
		input = strings.TrimSpace(input)
		tokens := strings.Split(input, " ")
		switch tokens[0] {
		case "help":
			cyan.Printf("Help:\n")
			fmt.Print("CDN application based on DHT. Usage:\n" +
				"help: show this help message\n" +
				"config: list the configuration of the application\n" +
				"list: show all files existing in the network\n" +
				"upload <filePath> <name>: upload <filePath> with name <name> to the network\n" +
				"download <name> <filePath>: download file <name> as <filePath>\n" +
				"exit: exit the system.\n")
		case "config":
			listConfig()
		case "list":
			listFiles()
		case "upload":
			uploadFile(tokens[1], tokens[2])
		case "download":
			downloadFile(tokens[1], tokens[2])
		case "exit":
			cyan.Println("Goodbye!")
			return
		}
	}
}
