package main

import (
	"dht/Kademlia"
)

/*
 * In this file, you need to implement the "NewNode" function.
 * This function should create a new DHT node and return it.
 * You can use the "naive.Node" struct as a reference to implement your own struct.
 */

//func NewNode(port int) dhtNode {
//	// Todo: create a node and then return it.
//	node := new(naive.Node)
//	node.Init(portToAddr(localAddress, port))
//	return node
//}

//	func NewNode(port int) dhtNode {
//		node := new(Chord.Node)
//		node.Init(portToAddr(localAddress, port))
//		return node
//	}
func NewNode(port int) dhtNode {
	node := new(Kademlia.Node)
	node.Init(portToAddr(localAddress, port))
	return node
}
