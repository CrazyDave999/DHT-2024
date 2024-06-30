package Chord

import (
	"crypto/sha256"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
}

const (
	M             int = 256
	R             int = 256 // length of successorList
	PingTime          = 100 * time.Millisecond
	StabilizeTime     = 100 * time.Millisecond
)

var LENGTH = new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(M)), nil) // length of the Chord ring

type IDType = big.Int

func hashString(s string) *big.Int {
	h := sha256.New()
	h.Write([]byte(s))
	hashed := h.Sum(nil)
	return new(big.Int).SetBytes(hashed)
}

type Meta struct {
	Id   *IDType
	Addr string
}

func (meta *Meta) Set(rhs *Meta) *Meta {
	meta.Addr = rhs.Addr
	meta.Id = new(IDType).Set(rhs.Id)
	return meta
}

type Node struct {
	meta              *Meta
	online            bool
	onlineLock        sync.RWMutex
	listener          net.Listener
	server            *rpc.Server
	data              map[string]string
	dataLock          sync.RWMutex
	backup            map[string]string
	backupLock        sync.RWMutex
	successorList     [R]*Meta
	successorListLock sync.RWMutex
	predecessor       *Meta
	predecessorLock   sync.RWMutex
	fingerTable       [M]*Meta
	fingerTableLock   sync.RWMutex
}
type Pair struct {
	Key   string
	Value string
}

// inRangeOO checks Id x is in (a, b)
func inRangeOO(x *IDType, a *IDType, b *IDType) bool {
	if a.Cmp(b) < 0 {
		return a.Cmp(x) < 0 && x.Cmp(b) < 0
	}
	return !inRangeCC(x, b, a)
}

// inRangeCC checks if x is in [a, b]
func inRangeCC(x *IDType, a *IDType, b *IDType) bool {
	if a.Cmp(b) <= 0 {
		return a.Cmp(x) <= 0 && x.Cmp(b) <= 0
	}
	return !inRangeOO(x, b, a)
}

// inRangeOC checks if x is in (a, b]
func inRangeOC(x *IDType, a *IDType, b *IDType) bool {
	if a == b {
		return false
	}
	if a.Cmp(b) < 0 {
		return a.Cmp(x) < 0 && x.Cmp(b) <= 0
	}
	return !inRangeOC(x, b, a)
}

// inRangeCO checks if x is in [a, b)
func inRangeCO(x *IDType, a *IDType, b *IDType) bool {
	if a == b {
		return false
	}
	if a.Cmp(b) < 0 {
		return a.Cmp(x) <= 0 && x.Cmp(b) < 0
	}
	return !inRangeCO(x, b, a)
}

func (node *Node) Init(addr string) {
	node.meta = &Meta{
		Id:   hashString(addr),
		Addr: addr,
	}

	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()

	node.successorListLock.Lock()
	for i := 0; i < R; i++ {
		node.successorList[i] = &Meta{
			Id:   new(IDType),
			Addr: "",
		}
	}
	node.successorListLock.Unlock()
	node.predecessorLock.Lock()
	node.predecessor = &Meta{
		Id:   new(IDType),
		Addr: "",
	}
	node.predecessorLock.Unlock()
	node.fingerTableLock.Lock()
	for i := 0; i < M; i++ {
		node.fingerTable[i] = &Meta{
			Id:   new(IDType),
			Addr: "",
		}
	}
	node.fingerTableLock.Unlock()
}

// RemoteCall calls the RPC method at Addr
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.meta.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, PingTime)
	if err != nil {
		logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

//
// RPC functions called by RemoteCall
//

func (node *Node) Ping(_ struct{}, _ *struct{}) error {
	return nil
}
func (node *Node) TryPing(addr string) bool {
	err := node.RemoteCall(addr, "Node.Ping", struct{}{}, nil)
	if err != nil {
		return true
	}
	return false
}

func (node *Node) RPCGetValue(key string, reply *string) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	v, ok := node.data[key]
	if ok {
		*reply = v
		return nil
	}
	*reply = ""
	return fmt.Errorf("[Error] Failed getting key: %v from node: %v. No such data exists", key, node.meta.Addr)
}

func (node *Node) RPCGetBackup(_ struct{}, reply *map[string]string) error {
	node.backupLock.RLock()
	defer node.backupLock.RUnlock()
	reply = &node.backup
	return nil
}

func (node *Node) RPCGetSuccessor(_ struct{}, reply *Meta) error {
	node.successorListLock.RLock()
	defer node.successorListLock.RUnlock()
	for i := 0; i < R; i++ {
		if node.TryPing(node.successorList[i].Addr) {
			reply.Set(node.successorList[i])
			return nil
		}
	}
	return fmt.Errorf("[Error] RPCGetSuccessor failed. All nodes in successorList are not online")
}

func (node *Node) RPCGetSucList(_ struct{}, reply *[R]*Meta) error {
	node.successorListLock.RLock()
	defer node.successorListLock.RUnlock()
	for i, v := range node.successorList {
		reply[i].Set(v)
	}
	return nil
}

func (node *Node) RPCGetPredecessor(_ struct{}, reply *Meta) error {
	node.predecessorLock.RLock()
	defer node.predecessorLock.RUnlock()
	reply.Set(node.predecessor)
	return nil
}

func (node *Node) RPCFindSuccessor(meta *Meta, reply *Meta) error {
	suc := &Meta{}
	err := node.RPCGetSuccessor(struct{}{}, suc)
	if err != nil {
		logrus.Errorf("[Error] RPCFindSuccessor failed when trying to node.RPCGetSuccessor(struct{}{}, suc) with node.meta: %v", *node.meta)
		return err
	}
	if inRangeOC(meta.Id, node.meta.Id, suc.Id) {
		reply.Set(suc)
		return nil
	}
	pred := &Meta{}
	err = node.RPCFindPredecessor(meta, pred)
	if err != nil {
		logrus.Errorf("[Error] RPCFindSuccessor failed when trying to node.RPCFindPredecessor(meta, meta1) with node.meta: %v, meta: %v", *node.meta, *meta)
		return err
	}
	return node.RemoteCall(pred.Addr, "Node.RPCGetSuccessor", struct{}{}, reply)
}
func (node *Node) RPCFindPredecessor(meta *Meta, reply *Meta) error {
	pred := (&Meta{}).Set(node.meta)
	suc := &Meta{}
	err := node.RemoteCall(pred.Addr, "Node.RPCGetSuccessor", struct{}{}, suc)
	if err != nil {
		return err
	}
	for !inRangeOC(meta.Id, pred.Id, suc.Id) {
		err = node.RemoteCall(pred.Addr, "Node.RPCClosestPrecedingFinger", meta, pred)
		if err != nil {
			return err
		}
		err = node.RemoteCall(pred.Addr, "Node.RPCGetSuccessor", struct{}{}, suc)
		if err != nil {
			return err
		}
	}
	reply.Set(pred)
	return nil
}
func (node *Node) RPCClosestPrecedingFinger(meta *Meta, reply *Meta) error {
	node.fingerTableLock.RLock()
	defer node.fingerTableLock.RUnlock()
	for i := M - 1; i >= 0; i-- {
		if node.TryPing(node.fingerTable[i].Addr) && inRangeOO(node.fingerTable[i].Id, node.meta.Id, meta.Id) {
			reply.Set(node.fingerTable[i])
			return nil
		}
	}
	reply.Set(node.meta)
	return nil
}

func (node *Node) Stabilize() error {
	suc := &Meta{}
	err := node.RPCGetSuccessor(struct{}{}, suc)
	if err != nil {
		logrus.Errorf("[Error] Stabilization failed when trying to node.RPCGetSuccessor")
		return err
	}
	pred := &Meta{}
	err = node.RemoteCall(suc.Addr, "Node.RPCGetPredecessor", struct{}{}, pred)
	if err != nil {
		logrus.Errorf("[Error] Stabilization failed when trying to node.RemoteCall(suc.Addr, \"Node.RPCGetPredecessor\", struct{}{}, pred)")
		return err
	}
	node.successorListLock.Lock()
	if inRangeOO(pred.Id, node.meta.Id, suc.Id) {
		suc = node.successorList[0].Set(pred)
	}
	var list *[R]*Meta
	err = node.RemoteCall(suc.Addr, "Node.RPCGetSucList", struct{}{}, list)
	if err != nil {
		logrus.Errorf("[Error] Stabilization failed when trying to get suc's successorList")
		return err
	}

	for i := 1; i < R; i++ {
		node.successorList[i].Set(list[i-1])
	}

	// check if successor is online.

	node.successorListLock.Unlock()

	err = node.RemoteCall(suc.Addr, "Node.RPCNotify", node.meta, nil)
	if err != nil {
		logrus.Errorf("[Error] Stabilization failed when trying to node.RemoteCall(suc.Addr, \"Node.RPCNotify\", node.meta, struct{}{})")
		return err
	}
	return nil
}
func (node *Node) RPCNotify(meta *Meta, _ *struct{}) error {
	node.predecessorLock.Lock()
	defer node.predecessorLock.Unlock()
	if node.predecessor.Addr == "" || inRangeOO(meta.Id, node.predecessor.Id, node.meta.Id) {
		node.predecessor.Set(meta)
	}
	return nil
}
func pow(i int64) *IDType {
	return new(IDType).Exp(big.NewInt(2), big.NewInt(i), nil)
}
func (node *Node) fingerStart(i int64) *IDType {
	return new(IDType).Mod(new(IDType).Add(node.meta.Id, pow(i)), LENGTH)
}
func (node *Node) FixFingers() error {
	rand.Seed(time.Now().UnixNano())
	rndIndex := int64(rand.Intn(M-1) + 1)
	node.fingerTableLock.Lock()
	defer node.fingerTableLock.Unlock()
	sucTmp := &Meta{}
	err := node.RPCFindSuccessor(&Meta{
		Id:   node.fingerStart(rndIndex),
		Addr: "",
	}, sucTmp)
	if err != nil {
		return err
	}

	node.fingerTable[rndIndex].Set(sucTmp)
	return nil
}

func (node *Node) CheckPredecessor() error {
	node.predecessorLock.Lock()
	if node.predecessor.Addr != "" && !node.TryPing(node.predecessor.Addr) {
		// predecessor failed. set predecessor = nil to let the stabilization correct it.
		// contents previously in node.backup now is in node's response. so they should be added to node's data.
		// since node's data changed, these contents should also be added to node.successor.backup.
		node.predecessor.Addr = ""
		node.predecessorLock.Unlock()
		node.backupLock.Lock()
		defer node.backupLock.Unlock()
		node.dataLock.Lock()
		for k, v := range node.backup {
			node.data[k] = v
		}
		node.dataLock.Unlock()
		suc := &Meta{}
		err := node.RPCGetSuccessor(struct{}{}, suc)
		if err != nil {
			return err
		}
		node.backupLock.Lock()
		err = node.RemoteCall(suc.Addr, "Node.RPCCopyToBackUp", node.backup, nil)
		if err != nil {
			return err
		}
		node.backup = make(map[string]string)
		node.backupLock.Unlock()
		return nil
	}
	node.predecessorLock.Unlock()
	return nil
}
func (node *Node) RPCCopyToBackup(data *map[string]string, _ *struct{}) error {
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	for k, v := range *data {
		node.backup[k] = v
	}
	return nil
}

func (node *Node) StartStabilize() {
	go func() {
		for node.online {
			err := node.Stabilize()
			if err != nil {
				return
			}
			time.Sleep(StabilizeTime)
		}
	}()
	go func() {
		for node.online {
			err := node.FixFingers()
			if err != nil {
				return
			}
			time.Sleep(StabilizeTime)
		}
	}()
	go func() {
		for node.online {
			err := node.CheckPredecessor()
			if err != nil {
				return
			}
			time.Sleep(StabilizeTime)
		}
	}()
}

func (node *Node) RPCPutInData(pair *Pair, _ *struct{}) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	node.data[pair.Key] = pair.Value
	return nil
}
func (node *Node) RPCPutInBackUp(pair *Pair, _ *struct{}) error {
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	node.backup[pair.Key] = pair.Value
	return nil
}

func (node *Node) RPCDeleteInData(key string, _ *struct{}) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	_, ok := node.data[key]
	if ok {
		delete(node.data, key)
		return nil
	} else {
		return fmt.Errorf("data not found in data: %v", key)
	}
}

func (node *Node) RPCDeleteInBackup(key string, _ *struct{}) error {
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	_, ok := node.backup[key]
	if ok {
		delete(node.backup, key)
		return nil
	} else {
		return fmt.Errorf("data not found in backup: %v", key)
	}
}

//
// DHT methods
//

func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		return
	}
	node.listener, err = net.Listen("tcp", node.meta.Addr)
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	node.onlineLock.Lock()
	node.online = true
	node.onlineLock.Unlock()
	go func() {
		for node.online {
			conn, err := node.listener.Accept()
			if err != nil {
				logrus.Error("accept error: ", err)
				return
			}
			go node.server.ServeConn(conn)
		}
	}()
}
func (node *Node) Create() {
	node.fingerTableLock.Lock()
	for i := 0; i < M; i++ {
		node.fingerTable[i].Set(node.meta)
	}
	node.fingerTableLock.Unlock()
	node.successorListLock.Lock()
	for i := 0; i < R; i++ {
		node.successorList[i].Set(node.meta)
	}
	node.successorListLock.Unlock()
	node.predecessorLock.Lock()
	node.predecessor.Set(node.meta)
	node.predecessorLock.Unlock()
	node.StartStabilize()
	logrus.Info("[Info] Hello from CrazyDave. Your network has been created successfully!")
}

func (node *Node) Join(addr string) bool {
	if !node.TryPing(addr) {
		logrus.Errorf("[Error] Join failed. With failure on Addr: %v", addr)
		return false
	}
	suc := &Meta{}
	err := node.RemoteCall(addr, "Node.RPCFindSuccessor", node.meta, suc)
	if err != nil {
		logrus.Errorf("[Error] Join failed when trying to Node.RPCFindSuccessor")
		return false
	}
	node.predecessorLock.Lock()
	node.predecessor = &Meta{
		Id:   new(IDType),
		Addr: "",
	}
	node.predecessorLock.Unlock()
	node.successorList[0].Set(suc)
	listPtr := &[R]*Meta{}
	err = node.RemoteCall(suc.Addr, "Node.RPCGetSucList", struct{}{}, listPtr)
	if err != nil {
		logrus.Errorf("[Error] Join failed when trying to Node.RPCGetSucList")
		return false
	}
	for i := 1; i < R; i++ {
		node.successorList[i].Set(listPtr[i-1])
	}
	node.StartStabilize()
	logrus.Infof("[Info] Node: %v joined by Addr: %v successfully.", node.meta.Addr, addr)
	return true
}
func (node *Node) Quit() {
	node.onlineLock.Lock()
	if !node.online {
		node.onlineLock.Unlock()
		return
	}
	err := node.listener.Close()
	if err != nil {
		logrus.Error("[Error] Quit failed: ", node.meta.Addr, err)
	}
	node.online = false
	node.onlineLock.Unlock()
}
func (node *Node) ForceQuit() {
	node.onlineLock.Lock()
	if !node.online {
		node.onlineLock.Unlock()
		return
	}
	err := node.listener.Close()
	if err != nil {
		logrus.Error("[Error] Quit failed: ", node.meta.Addr, err)
	}
	node.online = false
	node.onlineLock.Unlock()
}
func (node *Node) Put(key string, value string) bool {
	meta := &Meta{
		Addr: key,
		Id:   hashString(key),
	}
	suc := &Meta{}
	err := node.RPCFindSuccessor(meta, suc)
	if err != nil {
		return false
	}
	pair := &Pair{
		Key:   key,
		Value: value,
	}
	err = node.RemoteCall(suc.Addr, "Node.RPCPutInData", pair, nil)
	if err != nil {
		return false
	}
	sucSuc := &Meta{}
	err = node.RemoteCall(suc.Addr, "Node.RPCGetSuccessor", struct{}{}, sucSuc)
	if err != nil {
		return false
	}
	err = node.RemoteCall(sucSuc.Addr, "Node.RPCPutInBackup", pair, nil)
	if err != nil {
		return false
	}
	return true
}
func (node *Node) Get(key string) (ok bool, v string) {
	meta := &Meta{
		Id:   hashString(key),
		Addr: "",
	}
	suc := &Meta{}
	err := node.RPCFindSuccessor(meta, suc)
	if err != nil {
		ok = false
		v = ""
		return
	}
	err = node.RemoteCall(suc.Addr, "Node.RPCGetValue", key, &v)
	if err != nil {
		ok = false
		v = ""
		return
	}
	ok = true
	return
}
func (node *Node) Delete(key string) bool {
	meta := &Meta{
		Addr: key,
		Id:   hashString(key),
	}
	suc := &Meta{}
	err := node.RPCFindSuccessor(meta, suc)
	if err != nil {
		return false
	}
	err = node.RemoteCall(suc.Addr, "Node.RPCDeleteInData", key, nil)
	if err != nil {
		return false
	}
	sucSuc := &Meta{}
	err = node.RemoteCall(suc.Addr, "Node.RPCGetSuccessor", struct{}{}, sucSuc)
	if err != nil {
		return false
	}
	err = node.RemoteCall(sucSuc.Addr, "Node.RPCDeleteInBackup", key, nil)
	if err != nil {
		return false
	}
	return true
}
