package main

import (
	"CDN/Kademlia"
	"fmt"
	"github.com/fatih/color"
)

var (
	green  = color.New(color.FgGreen)
	red    = color.New(color.FgRed)
	yellow = color.New(color.FgYellow)
	cyan   = color.New(color.FgCyan)
)

func portToAddr(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func NewNode(port int) *Kademlia.Node {
	node := new(Kademlia.Node)
	node.Init(portToAddr(localAddress, port))
	return node
}

const NodeSize int = 50
const firstPort int = 20000

var localAddress = "127.0.0.1"
