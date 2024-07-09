package Chord

import (
	"crypto/sha1"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
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
	M             int = 160
	R             int = 10 // length of successorList
	PingTime          = 100 * time.Millisecond
	StabilizeTime     = 50 * time.Millisecond
)

var LENGTH = new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(M)), nil) // length of the Chord ring

type IDType = big.Int

func hashString(s string) *big.Int {
	h := sha1.New()
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
	fixIndex          int64
	fingerStart       [M]*IDType
	block             sync.Mutex
}
type Pair struct {
	Key   string
	Value string
}

// inRangeOO checks Id x is in (a, b)
func inRangeOO(x *IDType, a *IDType, b *IDType) bool {
	if a.Cmp(b) == 0 {
		return false
	}
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
	if a.Cmp(b) == 0 {
		return false
	}
	if a.Cmp(b) < 0 {
		return a.Cmp(x) < 0 && x.Cmp(b) <= 0
	}
	return !inRangeOC(x, b, a)
}

// inRangeCO checks if x is in [a, b)
func inRangeCO(x *IDType, a *IDType, b *IDType) bool {
	if a.Cmp(b) == 0 {
		return false
	}
	if a.Cmp(b) < 0 {
		return a.Cmp(x) <= 0 && x.Cmp(b) < 0
	}
	return !inRangeCO(x, b, a)
}

func (node *Node) LockBlock(_ struct{}, _ *struct{}) error {
	node.block.Lock()
	return nil
}
func (node *Node) UnLockBlock(_ struct{}, _ *struct{}) error {
	node.block.Unlock()
	return nil
}

func (node *Node) Init(addr string) {
	node.meta = &Meta{
		Id:   hashString(addr),
		Addr: addr,
	}
	node.data = make(map[string]string)
	node.backup = make(map[string]string)

	for i := 0; i < R; i++ {
		node.successorList[i] = &Meta{
			Id:   new(IDType),
			Addr: "",
		}
	}
	node.predecessor = &Meta{
		Id:   new(IDType),
		Addr: "",
	}

	for i := 0; i < M; i++ {
		node.fingerTable[i] = &Meta{
			Id:   new(IDType),
			Addr: "",
		}
		node.fingerStart[i] = new(IDType).Mod(new(IDType).Add(node.meta.Id, new(IDType).Exp(big.NewInt(2), big.NewInt(int64(i)), nil)), LENGTH)
	}
	node.fixIndex = 0
}

// RemoteCall calls the RPC method at Addr
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", addr, PingTime)
	if err != nil {
		//logrus.Errorf("[%s] dialing: %s, %s", node.meta.Addr, addr, err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		//logrus.Errorf("[%s] RemoteCall error: %s", node.meta.Addr, err)
		return err
	}
	return nil
}

//
// RPC functions called by RemoteCall
//

func (node *Node) Ping(_ struct{}, _ *struct{}) error {
	if !node.online {

		return fmt.Errorf("damn! offline")
	}
	return nil
}
func (node *Node) TryPing(addr string) bool {
	err := node.RemoteCall(addr, "Node.Ping", struct{}{}, nil)
	return err == nil
}

func (node *Node) RPCGetValue(key string, reply *string) error {
	node.dataLock.RLock()
	v, ok := node.data[key]
	node.dataLock.RUnlock()
	if ok {
		*reply = v
		return nil
	}

	node.backupLock.RLock()
	v, ok = node.backup[key]
	node.backupLock.RUnlock()
	if ok {
		*reply = v
		return nil
	}

	*reply = ""
	return fmt.Errorf("[%s] RPCGetValue. Failed getting key: %v. No such data exists", node.meta.Addr, key)
}

func (node *Node) RPCGetData(_ struct{}, reply *map[string]string) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	for k, v := range node.data {
		(*reply)[k] = v
	}
	return nil
}

func (node *Node) RPCGetBackup(_ struct{}, reply *map[string]string) error {
	node.backupLock.RLock()
	defer node.backupLock.RUnlock()
	for k, v := range node.backup {
		(*reply)[k] = v
	}
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
	return fmt.Errorf("RPCGetSuccessor failed. All nodes in successorList are not online")
}

func (node *Node) RPCGetSucList(_ struct{}, reply *[R]*Meta) error {
	node.successorListLock.RLock()
	defer node.successorListLock.RUnlock()
	for i, v := range node.successorList {
		reply[i] = new(Meta).Set(v)
	}
	return nil
}

func (node *Node) RPCGetPredecessor(_ struct{}, reply *Meta) error {
	node.predecessorLock.RLock()
	if node.predecessor.Addr != "" && !node.TryPing(node.predecessor.Addr) {
		node.predecessorLock.RUnlock()
		node.predecessorLock.Lock()
		node.predecessor.Addr = ""
		node.predecessorLock.Unlock()
		reply.Set(&Meta{
			Id:   new(IDType),
			Addr: "",
		})
		return nil
	}
	reply.Set(node.predecessor)
	node.predecessorLock.RUnlock()
	return nil
}

func (node *Node) RPCFindSuccessor(meta *Meta, reply *Meta) error {
	if node.meta.Id.Cmp(meta.Id) == 0 {
		reply.Set(node.meta)
		return nil
	}
	suc := &Meta{}
	//logrus.Infof("[%s] RPCFindSuccessor. RPCGetSuccessor begins.", node.meta.Addr)
	err := node.RPCGetSuccessor(struct{}{}, suc)
	if err != nil {
		//logrus.Errorf("RPCFindSuccessor failed when trying to RPCGetSuccessor")
		return err
	}
	//logrus.Infof("[%s] RPCFindSuccessor. RPCGetSuccessor ends.", node.meta.Addr)
	if node.meta.Addr == suc.Addr || inRangeOC(meta.Id, node.meta.Id, suc.Id) {
		reply.Set(suc)
		return nil
	}
	pred := &Meta{}
	//logrus.Infof("[%s] RPCFindSuccessor. RPCFindPredecessor begins.", node.meta.Addr)
	err = node.RPCFindPredecessor(meta, pred)
	if err != nil {
		//logrus.Errorf("RPCFindSuccessor failed when trying to RPCFindPredecessor")
		return err
	}
	//logrus.Infof("[%s] RPCFindSuccessor. RPCFindPredecessor ends. pred: %s", node.meta.Addr, pred.Addr)
	//logrus.Infof("[%s] RPCFindSuccessor. RPCGetSuccessor begins.", node.meta.Addr)
	err = node.RemoteCall(pred.Addr, "Node.RPCGetSuccessor", struct{}{}, reply)
	//logrus.Infof("[%s] RPCFindSuccessor. RPCGetSuccessor ends.", node.meta.Addr)
	return err
}
func (node *Node) RPCFindPredecessor(meta *Meta, reply *Meta) error {
	//logrus.Infof("[%s] RPCFindPredecessor. meta: %s", node.meta.Addr, meta.Addr)
	pred := (&Meta{}).Set(node.meta)
	suc := &Meta{}
	err := node.RPCGetSuccessor(struct{}{}, suc)
	if err != nil {
		return err
	}
	if inRangeOC(meta.Id, node.meta.Id, suc.Id) {
		reply.Set(node.meta)
		return nil
	} else {
		//logrus.Infof("[%s] RPCFindPredecessor. RPCClosestPrecedingFinger begins. meta: %s", node.meta.Addr, meta.Addr)
		err = node.RPCClosestPrecedingFinger(meta, pred)
		//logrus.Infof("[%s] RPCFindPredecessor. RPCClosestPrecedingFinger ends. meta: %s, pred: %s", node.meta.Addr, meta.Addr, pred.Addr)
		if err != nil {
			//logrus.Errorf("[%s] RPCFindPredecessor. RPCClosestPrecedingFinger failed. meta: %s", node.meta.Addr, meta.Addr)
			return err
		}
		return node.RemoteCall(pred.Addr, "Node.RPCFindPredecessor", meta, reply)
	}
}
func (node *Node) RPCClosestPrecedingFinger(meta *Meta, reply *Meta) error {
	for i := M - 1; i >= 0; i-- {
		node.fingerTableLock.RLock()
		finger := node.fingerTable[i]
		node.fingerTableLock.RUnlock()
		if finger.Addr != "" && inRangeOO(finger.Id, node.meta.Id, meta.Id) {
			if node.TryPing(finger.Addr) {
				reply.Set(finger)
				return nil
			}
		}
	}
	reply.Set(node.meta)
	return nil
}

func (node *Node) Stabilize() error {
	//logrus.Infof("[%s] Stabilize begins", node.meta.Addr)
	suc := &Meta{}
	//logrus.Infof("[%s] Stabilize. RPCGetSuccessor begins", node.meta.Addr)
	err := node.RPCGetSuccessor(struct{}{}, suc)
	if err != nil {
		//logrus.Errorf("[%s] Stabilization failed when trying to node.RPCGetSuccessor", node.meta.Addr)
		return err
	}
	//logrus.Infof("[%s] Stabilize. RPCGetSuccessor ends. suc: %s", node.meta.Addr, suc.Addr)
	pred := &Meta{}
	//logrus.Infof("[%s] Stabilize RPCGetPredecessor begins", node.meta.Addr)
	err = node.RemoteCall(suc.Addr, "Node.RPCGetPredecessor", struct{}{}, pred)
	if err != nil {
		//logrus.Errorf("[%s] Stabilization failed when trying to RPCGetPredecessor. suc: %s", node.meta.Addr, suc.Addr)
		return err
	}
	//logrus.Infof("[%s] Stabilize RPCGetPredecessor ends. pred: %s", node.meta.Addr, pred.Addr)

	if pred.Addr != "" && (suc.Addr == node.meta.Addr || inRangeOO(pred.Id, node.meta.Id, suc.Id)) {
		//logrus.Infof("[%s] RPCTransferData begins", node.meta.Addr)

		err = node.RemoteCall(suc.Addr, "Node.RPCTransferForNewSuccessor", pred, nil)
		//logrus.Infof("[%s] RPCTransferData ends", node.meta.Addr)
		node.successorListLock.Lock()
		for i := R - 1; i >= 1; i-- {
			node.successorList[i].Set(node.successorList[i-1])
		}
		suc = node.successorList[0].Set(pred)
		node.successorListLock.Unlock()
		node.fingerTableLock.Lock()
		node.fingerTable[0].Set(suc)
		node.fingerTableLock.Unlock()
	} else {
		node.successorListLock.Lock()
		if suc.Addr != node.successorList[0].Addr {
			node.successorList[0].Set(suc)
			node.successorListLock.Unlock()
			node.fingerTableLock.Lock()
			node.fingerTable[0].Set(suc)
			node.fingerTableLock.Unlock()
			node.dataLock.RLock()
			data := make(map[string]string)
			for k, v := range node.data {
				data[k] = v
			}
			node.dataLock.RUnlock()
			err = node.RemoteCall(suc.Addr, "Node.RPCTransferForSuccessorFail", data, nil)
		} else {
			node.successorListLock.Unlock()
		}

		list := &[R]*Meta{}

		//logrus.Infof("[%s] RPCGetSucList begins", node.meta.Addr)
		err = node.RemoteCall(suc.Addr, "Node.RPCGetSucList", struct{}{}, list)
		if err != nil {
			//logrus.Errorf("[%s] Stabilization failed when trying to get suc's successorList", node.meta.Addr)
			return err
		}
		//logrus.Infof("[%s] RPCGetSucList ends", node.meta.Addr)
		node.successorListLock.Lock()
		for i := 1; i < R; i++ {
			node.successorList[i].Set(list[i-1])
		}
		node.successorListLock.Unlock()
	}

	err = node.RemoteCall(suc.Addr, "Node.RPCNotify", node.meta, nil)

	if err != nil {
		//logrus.Errorf("Stabilization failed when trying to Node.RPCNotify")
		return err
	}

	//node.successorListLock.RLock()
	//sucList := [R]string{}
	//for i := 0; i < R; i++ {
	//	l := len(node.successorList[i].Addr)
	//	if l < 2 {
	//		continue
	//	}
	//	sucList[i] = node.successorList[i].Addr[l-2:]
	//}
	//node.successorListLock.RUnlock()
	//logrus.Infof("[%s] sucList: %s", node.meta.Addr, sucList)

	return nil
}

func (node *Node) RPCTransferForQuit(backup map[string]string, _ *struct{}) error {
	// step 1
	node.backupLock.RLock()
	data := make(map[string]string)
	for k, v := range node.backup {
		data[k] = v
	}
	node.backupLock.RUnlock()
	node.dataLock.Lock()
	//logrus.Infof("[%s] RPCTransferForQuit. node.data: %s, node.backup: %s, backup: %s", node.meta.Addr, node.data, data, backup)
	for k, v := range data {
		node.data[k] = v
	}
	node.dataLock.Unlock()

	// step 2
	n3 := &Meta{}
	err := node.RPCGetSuccessor(struct{}{}, n3)
	if err != nil {
		return err
	}
	err = node.RemoteCall(n3.Addr, "Node.RPCCopyToBackup", data, nil)
	if err != nil {
		return err
	}

	// step 3
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for k, v := range backup {
		node.backup[k] = v
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) RPCTransferForNewSuccessor(n1 *Meta, _ *struct{}) error {
	n2 := node.meta
	// step 1
	node.backupLock.RLock()
	data := make(map[string]string)
	for k, v := range node.backup {
		data[k] = v
	}
	node.backupLock.RUnlock()
	err := node.RemoteCall(n1.Addr, "Node.RPCSetBackup", data, nil)
	if err != nil {
		return err
	}

	// step 2
	node.dataLock.Lock()
	data = make(map[string]string)
	for k, v := range node.data {
		if !inRangeOC(hashString(k), n1.Id, n2.Id) {
			data[k] = v
		}
	}
	for k := range data {
		delete(node.data, k)
	}
	node.dataLock.Unlock()

	err = node.RemoteCall(n1.Addr, "Node.RPCCopyToData", data, nil)
	if err != nil {
		return err
	}

	// step 3
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for k, v := range data {
		node.backup[k] = v
	}
	node.backupLock.Unlock()

	// step 4
	n3 := &Meta{}
	err = node.RPCGetSuccessor(struct{}{}, n3)
	if err != nil {
		return err
	}
	err = node.RemoteCall(n3.Addr, "Node.RPCRemoveFromBackup", data, nil)
	if err != nil {
		return err
	}
	return nil
}

func (node *Node) RPCTransferForSuccessorFail(data map[string]string, _ *struct{}) error {
	// step 1
	node.backupLock.RLock()
	backup := make(map[string]string)
	for k, v := range node.backup {
		backup[k] = v
	}
	node.backupLock.RUnlock()
	node.dataLock.Lock()
	for k, v := range backup {
		node.data[k] = v
	}
	node.dataLock.Unlock()

	// step 2
	n3 := &Meta{}
	err := node.RPCGetSuccessor(struct{}{}, n3)
	if err != nil {
		return err
	}
	err = node.RemoteCall(n3.Addr, "Node.RPCCopyToBackup", backup, nil)
	if err != nil {
		return err
	}

	// step 3
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for k, v := range data {
		node.backup[k] = v
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) RPCNotify(meta *Meta, _ *struct{}) error {
	node.predecessorLock.Lock()
	defer node.predecessorLock.Unlock()
	if node.predecessor.Addr == "" || node.predecessor.Addr == node.meta.Addr || inRangeOO(meta.Id, node.predecessor.Id, node.meta.Id) {
		node.predecessor.Set(meta)
	}
	return nil
}

func (node *Node) FixFingers() error {

	sucTmp := &Meta{}

	err := node.RPCFindSuccessor(&Meta{
		Id:   new(IDType).Set(node.fingerStart[node.fixIndex]),
		Addr: "",
	}, sucTmp)
	if err != nil {
		return err
	}
	node.fingerTableLock.Lock()
	defer node.fingerTableLock.Unlock()
	node.fingerTable[node.fixIndex].Set(sucTmp)
	node.fixIndex++
	node.fixIndex %= int64(M)
	return nil
}

func (node *Node) RPCCopyToData(data map[string]string, _ *struct{}) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	for k, v := range data {
		node.data[k] = v
	}
	return nil
}

func (node *Node) RPCCopyToBackup(data map[string]string, _ *struct{}) error {
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	for k, v := range data {
		node.backup[k] = v
	}
	return nil
}

func (node *Node) RPCRemoveFromBackup(data map[string]string, _ *struct{}) error {
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	for k := range data {
		delete(node.backup, k)
	}
	return nil
}

func (node *Node) RPCSetBackup(data map[string]string, _ *struct{}) error {
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	node.backup = make(map[string]string)
	for k, v := range data {
		node.backup[k] = v
	}
	return nil
}

func (node *Node) StartStabilize() {
	go func() {
		for {
			node.onlineLock.RLock()
			if !node.online {
				node.onlineLock.RUnlock()
				return
			}
			node.onlineLock.RUnlock()

			node.block.Lock()
			err := node.Stabilize()
			if err != nil {
				//return
			}
			node.block.Unlock()
			time.Sleep(StabilizeTime)
		}
	}()
	go func() {
		for {
			node.onlineLock.RLock()
			if !node.online {
				node.onlineLock.RUnlock()
				return
			}
			node.onlineLock.RUnlock()
			err := node.FixFingers()
			if err != nil {
				//return
			}
			time.Sleep(StabilizeTime)
		}
	}()
}

func (node *Node) RPCPutInData(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	node.data[pair.Key] = pair.Value
	return nil
}
func (node *Node) RPCPutInBackup(pair Pair, _ *struct{}) error {
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

func (node *Node) RPCSetSuccessorList(list [R]*Meta, _ *struct{}) error {
	node.successorListLock.Lock()
	for i, v := range list {
		node.successorList[i].Set(v)
	}
	node.successorListLock.Unlock()
	node.fingerTableLock.Lock()
	node.fingerTable[0].Set(list[0])
	node.fingerTableLock.Unlock()
	return nil
}

func (node *Node) RPCSetPredecessor(pred *Meta, _ *struct{}) error {
	node.predecessorLock.Lock()
	node.predecessor.Set(pred)
	node.predecessorLock.Unlock()
	return nil
}

func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		return
	}
	node.listener, err = net.Listen("tcp", node.meta.Addr)
	if err != nil {
		//logrus.Fatal("listen error: ", err)
	}
	node.onlineLock.Lock()
	node.online = true
	node.onlineLock.Unlock()
	//logrus.Info("Run successfully. ", node.meta.Addr)
	go func() {
		for node.online {
			conn, err := node.listener.Accept()
			if err != nil {
				//logrus.Error("accept error: ", err)
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
	//node.fingerTable[0].Set(node.meta)
	node.fingerTableLock.Unlock()
	node.successorListLock.Lock()
	for i := 0; i < R; i++ {
		node.successorList[i].Set(node.meta)
	}
	//node.successorList[0].Set(node.meta)
	node.successorListLock.Unlock()
	node.predecessorLock.Lock()
	node.predecessor.Set(node.meta)
	node.predecessorLock.Unlock()
	node.StartStabilize()
	//logrus.Info("[Info] Hello from CrazyDave. Your network has been created successfully!")
}

func (node *Node) Join(addr string) bool {
	//logrus.Info("I am node: ", node.meta.Addr, " now I am trying to Join! With help of: ", addr)

	suc := &Meta{}
	//logrus.Infof("[%s] Joining. RPCFindSuccessor begins.", node.meta.Addr)
	err := node.RemoteCall(addr, "Node.RPCFindSuccessor", node.meta, suc)
	//logrus.Infof("[%s] Joining. RPCFindSuccessor ends. suc: %s", node.meta.Addr, suc.Addr)
	if err != nil {
		//logrus.Errorf("Join failed when trying to Node.RPCFindSuccessor")
		return false
	}
	node.predecessorLock.Lock()
	node.predecessor = &Meta{
		Id:   new(IDType),
		Addr: "",
	}
	node.predecessorLock.Unlock()
	node.successorListLock.Lock()
	node.successorList[0].Set(suc)
	node.successorListLock.Unlock()
	list := &[R]*Meta{}
	//logrus.Infof("[%s] Joining. RPCGetSucList begins.", node.meta.Addr)
	err = node.RemoteCall(suc.Addr, "Node.RPCGetSucList", struct{}{}, list)
	if err != nil {
		//logrus.Errorf("Join failed when trying to Node.RPCGetSucList")
		return false
	}
	//logrus.Infof("[%s] Joining. RPCGetSucList ends.", node.meta.Addr)
	node.successorListLock.Lock()
	for i := 1; i < R; i++ {
		node.successorList[i].Set(list[i-1])
	}
	node.successorListLock.Unlock()
	node.StartStabilize()
	//logrus.Infof("Node: %v joined by Addr: %v successfully.", node.meta.Addr, addr)
	return true
}
func (node *Node) Quit() {
	if !node.online {
		return
	}

	node.block.Lock()
	list := [R]*Meta{}
	//logrus.Infof("[%s] Quit. RPCGetSucList begins.", node.meta.Addr)
	err := node.RPCGetSucList(struct{}{}, &list)
	if err != nil {
		//logrus.Error("GetSucList failed.")
	}
	//logrus.Infof("[%s] Quit. RPCGetSucList ends.", node.meta.Addr)
	suc := list[0]
	node.predecessorLock.RLock()
	pred := node.predecessor
	if pred.Addr != "" && !node.TryPing(pred.Addr) {
		pred.Addr = ""
	}
	node.predecessorLock.RUnlock()

	if suc.Addr != node.meta.Addr {
		//logrus.Infof("[%s] Quit. LockBlock begins. suc: %s", node.meta.Addr, suc.Addr)
		err = node.RemoteCall(suc.Addr, "Node.LockBlock", struct{}{}, nil)
		//logrus.Infof("[%s] Quit. LockBlock ends. suc: %s", node.meta.Addr, suc.Addr)
	}
	if pred.Addr != "" && pred.Addr != node.meta.Addr && pred.Addr != suc.Addr {
		//logrus.Infof("[%s] Quit. LockBlock begins. pred: %s", node.meta.Addr, pred.Addr)
		err = node.RemoteCall(pred.Addr, "Node.LockBlock", struct{}{}, nil)
		//logrus.Infof("[%s] Quit. LockBlock ends. pred: %s", node.meta.Addr, pred.Addr)
	}

	backup := make(map[string]string)
	node.backupLock.RLock()
	for k, v := range node.backup {
		backup[k] = v
	}
	node.backupLock.RUnlock()

	//logrus.Infof("[%s] Quit. RPCTransferForQuit begins. suc: %s, pred: %s, ", node.meta.Addr, suc.Addr, pred.Addr)
	err = node.RemoteCall(suc.Addr, "Node.RPCTransferForQuit", backup, nil)
	//logrus.Infof("[%s] Quit. RPCTransferForQuit ends. suc: %s, pred: %s", node.meta.Addr, suc.Addr, pred.Addr)

	err = node.RemoteCall(pred.Addr, "Node.RPCSetSuccessorList", list, nil)

	err = node.RemoteCall(suc.Addr, "Node.RPCSetPredecessor", pred, nil)

	err = node.listener.Close()
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	if suc.Addr != node.meta.Addr {
		err = node.RemoteCall(suc.Addr, "Node.UnLockBlock", struct{}{}, nil)
	}
	if pred.Addr != "" && pred.Addr != node.meta.Addr && pred.Addr != suc.Addr {
		err = node.RemoteCall(pred.Addr, "Node.UnLockBlock", struct{}{}, nil)
	}
	//logrus.Infof("[%s] Quit successfully.", node.meta.Addr)
}
func (node *Node) ForceQuit() {
	if !node.online {
		return
	}
	err := node.listener.Close()
	if err != nil {
		//logrus.Error("Quit failed: ", node.meta.Addr, err)
	}
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	//logrus.Infof("[%s] ForceQuit successfully.", node.meta.Addr)
}
func (node *Node) Put(key string, value string) bool {
	//logrus.Infof("[%s] Put begins %v, %v", node.meta.Addr, key, value)
	meta := &Meta{
		Addr: "",
		Id:   hashString(key),
	}
	suc := &Meta{}
	//logrus.Infof("[%s] Put. RPCFindSuccessor begins.", node.meta.Addr)
	err := node.RPCFindSuccessor(meta, suc)
	if err != nil {
		//logrus.Errorf("[%s] Put. RPCFindSuccessor failed.", node.meta.Addr)
		return false
	}
	//logrus.Infof("[%s] Put. RPCFindSuccessor ends. suc: %s", node.meta.Addr, suc.Addr)
	pair := Pair{
		Key:   key,
		Value: value,
	}
	//logrus.Infof("[%s] Put. RPCPutInData begins", node.meta.Addr)
	err = node.RemoteCall(suc.Addr, "Node.RPCPutInData", pair, nil)
	if err != nil {
		//logrus.Errorf("[%s] Put. RPCPutInData failed.", node.meta.Addr)
		return false
	}
	//logrus.Infof("[%s] Put. RPCPutInData ends", node.meta.Addr)
	sucSuc := &Meta{}
	//logrus.Infof("[%s] Put. RPCGetSuccessor begins. suc: %s, sucSuc: %s", node.meta.Addr, suc.Addr, sucSuc.Addr)
	err = node.RemoteCall(suc.Addr, "Node.RPCGetSuccessor", struct{}{}, sucSuc)
	//logrus.Infof("[%s] Put. RPCGetSuccessor ends. suc: %s, sucSuc: %s", node.meta.Addr, suc.Addr, sucSuc.Addr)
	if err != nil {
		//logrus.Error("Put failed when trying to Node.RPCGetSuccessor")
	} else {
		//logrus.Infof("[%s] Put. RPCPutInBackup begins. suc: %s, sucSuc: %s", node.meta.Addr, suc.Addr, sucSuc.Addr)
		err = node.RemoteCall(sucSuc.Addr, "Node.RPCPutInBackup", pair, nil)
		//logrus.Infof("[%s] Put. RPCPutInBackup ends. suc: %s, sucSuc: %s", node.meta.Addr, suc.Addr, sucSuc.Addr)
		if err != nil {
			//logrus.Error("Put failed when trying to Node.RPCPutInBackup.")
		}
	}
	logrus.Infof("[%s] Put key: %v successfully. suc: %s, sucSuc: %s", node.meta.Addr, key, suc.Addr, sucSuc.Addr)
	return true
}
func (node *Node) Get(key string) (ok bool, v string) {
	meta := &Meta{
		Id:   hashString(key),
		Addr: "",
	}
	suc := &Meta{}
	//logrus.Infof("[%s] Get. RPCFindSuccessor begins", node.meta.Addr)
	err := node.RPCFindSuccessor(meta, suc)
	//logrus.Infof("[%s] Get. RPCFindSuccessor ends. suc: %s", node.meta.Addr, suc.Addr)
	if err != nil {
		ok = false
		v = ""
		//logrus.Errorf("[%s] Get failed when trying to find the successor of key: %v", node.meta.Addr, key)
		return
	}
	//logrus.Infof("[%s] Get. RPCGetValue begins", node.meta.Addr)
	err = node.RemoteCall(suc.Addr, "Node.RPCGetValue", key, &v)
	//logrus.Infof("[%s] Get. RPCGetValue ends", node.meta.Addr)
	if err != nil {
		ok = false
		v = ""
		logrus.Errorf("[%s] Get failed when trying to RPCGetValue from: %s", node.meta.Addr, suc.Addr)
		return
	}
	ok = true
	return
}
func (node *Node) Delete(key string) bool {
	meta := &Meta{
		Addr: "",
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
		//logrus.Error("Delete failed when trying to Node.RPCGetSuccessor")
	} else {
		err = node.RemoteCall(sucSuc.Addr, "Node.RPCDeleteInBackup", key, nil)
		if err != nil {
			//logrus.Error("Delete failed when trying to Node.RPCDeleteInBackup")
		}
	}
	//logrus.Infof("[%s] Delete key: %v successfully. suc: %s, sucSuc: %s", node.meta.Addr, key, suc.Addr, sucSuc.Addr)
	return true
}
