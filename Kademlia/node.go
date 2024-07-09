package Kademlia

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var pow [M]*big.Int

type Node struct {
	ip         string
	id         *big.Int
	online     bool
	rt         *RoutingTable
	onlineLock sync.RWMutex
	listener   net.Listener
	server     *rpc.Server
	db         DataBase
	//RefreshIndex int
}

func init() {
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
	for i := 0; i < M; i++ {
		pow[i] = new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i)), nil)
	}
}

func RemoteCall(ip string, method string, args interface{}, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", ip, PingTime)
	if err != nil {
		logrus.Errorf("Remote call failed. Dialing ip: %s, method: %s, error: %s", ip, method, err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Errorf("Remote call failed. ip: %s, method: %s, error: %s", ip, method, err)
		return err
	}
	return nil
}
func (node *Node) RemoteCall(ip string, method string, args interface{}, reply interface{}) error {
	err := RemoteCall(ip, method, args, reply)

	// piggybacked update operation on the sender
	if method != "Node.Ping" && method != "Node.Fetch" {
		node.Update(ip, err == nil)
	}
	return err
}

func (node *Node) Init(addr string) {
	node.rt = NewRoutingTable()
	node.ip = addr
	node.id = new(big.Int).Set(hashString(addr))
	node.db.Init()
}

func (node *Node) WhichBucket(id *big.Int) int {
	dis := new(big.Int).Xor(node.id, id)
	for i := M - 1; i >= 0; i-- {
		if dis.Cmp(pow[i]) >= 0 {
			return i
		}
	}
	return -1
}

func (node *Node) Ping(_ struct{}, _ *struct{}) error {
	return nil
}

func TryPing(ip string) bool {
	err := RemoteCall(ip, "Node.Ping", struct{}{}, nil)
	return err == nil
}
func (node *Node) Store(pair *Pair, _ *struct{}) error {
	node.db.Put(pair.Key, pair.Value)
	return nil
}
func (node *Node) Fetch(key string, value *string) error {
	v, ok := node.db.Get(key)
	if ok {
		*value = v
		return nil
	}
	*value = ""
	return fmt.Errorf("no such key exist")
}

func (node *Node) Update(ip string, online bool) {
	if ip == node.ip {
		return
	}
	ind := node.WhichBucket(hashString(ip))
	if ind != -1 {
		node.rt.Update(ind, ip, online)
	}
}

// FindNode 返回自己路由表中距离id最近的k个节点ip
func (node *Node) FindNode(id *big.Int, reply *[]string) error {
	var done chan bool
	go func() {
		ind := node.WhichBucket(id)
		if ind == -1 {
			*reply = append(*reply, node.ip)
		} else {
			buc := node.rt.GetNodes(ind)
			for _, v := range buc {
				*reply = append(*reply, v)
			}
		}
		if len(*reply) == K {
			done <- true
			return
		}
		for i := ind + 1; i < M; i++ {
			buc := node.rt.GetNodes(i)
			for _, v := range buc {
				*reply = append(*reply, v)
				if len(*reply) == K {
					done <- true
					return
				}
			}
		}
		for i := ind - 1; i >= 0; i-- {
			buc := node.rt.GetNodes(i)
			for _, v := range buc {
				*reply = append(*reply, v)
				if len(*reply) == K {
					done <- true
					return
				}
			}
		}
		if ind != -1 {
			*reply = append(*reply, node.ip)
		}
	}()
	select {
	case <-done:
		return nil
	case <-time.After(FindNodeTimeout):
		return nil
	}
}

// NodeLookup 返回距离id最近的k个ip
func (node *Node) NodeLookup(id *big.Int) (kClosest []string) {
	var sl ShortList
	sl.Init(id)
	done := make(chan bool)
	go func() {
		findList := make([]string, 0)
		err := node.FindNode(id, &findList)
		if err != nil {
			return
		}
		for _, v := range findList {
			sl.Insert(v)
		}

		// 每轮循环，从shortList中选取alpha个没有被询问过的，并发地对它们调用FindNode，得到的节点都加入到shortList
		// 如果一个节点未能在规定时间内返回，则从shortList中移除
		// 如果在一轮循环中最接近的节点未被改变，则对前k个中还没问过的全部节点执行FindNode
		for {
			queryList := sl.GetAlphaNotQueried()
			findList = make([]string, 0)
			node.MakeQuery(id, &queryList, &findList, &sl)
			changed := sl.Update(findList)
			if !changed {
				queryList = sl.GetAllNotQueried()
				findList = make([]string, 0)
				node.MakeQuery(id, &queryList, &findList, &sl)
				changed = sl.Update(findList)
				if !changed {
					break
				}
			}
		}
		done <- true
	}()
	select {
	case <-done:
		return sl.GetKClosest()
	case <-time.After(NodeLookupTimeout):
		return sl.GetKClosest()
	}
}

func (node *Node) MakeQuery(id *big.Int, queryList *[]string, findList *[]string, sl *ShortList) {
	var wg sync.WaitGroup
	wg.Add(len(*queryList))
	findListLock := sync.Mutex{}
	for _, ip := range *queryList {
		go func(ip string) {
			defer wg.Done()
			reply := make([]string, 0)
			err := node.RemoteCall(ip, "Node.FindNode", id, &reply)
			if err != nil {
				sl.Remove(ip)
				return
			}
			sl.SetQueried(ip)
			findListLock.Lock()
			*findList = append(*findList, reply...)
			findListLock.Unlock()
		}(ip)
	}
	wg.Wait()
}

func (node *Node) MakeFetch(key string, queryList *[]string, findList *[]string, sl *ShortList) (ok bool, v string) {
	ok, v = false, ""
	id := hashString(key)
	var wg sync.WaitGroup
	wg.Add(len(*queryList))
	findListLock := sync.Mutex{}
	resLock := sync.Mutex{}
	for _, ip := range *queryList {
		go func(ip string) {
			defer wg.Done()
			reply := make([]string, 0)
			err := node.RemoteCall(ip, "Node.FindNode", id, &reply)
			if err != nil {
				sl.Remove(ip)
				return
			}
			sl.SetQueried(ip)
			findListLock.Lock()
			*findList = append(*findList, reply...)
			findListLock.Unlock()
			var val string
			err = RemoteCall(ip, "Node.Fetch", key, &val)
			resLock.Lock()
			if err == nil && ok == false {
				ok, v = true, val
			}
			resLock.Unlock()
		}(ip)
	}
	wg.Wait()
	return
}

func (node *Node) RepublishPairList(pairs []Pair) {
	logrus.Infof("[%s] RepublishPairList begins. pairs: %s", node.ip, pairs)
	var wg sync.WaitGroup
	wg.Add(len(pairs))
	for _, pair := range pairs {
		go func(pair Pair) {
			done := make(chan bool)
			go node.RepublishPair(pair, done)
			select {
			case <-done:
				wg.Done()
			case <-time.After(RepublishPairTimeOut):
				wg.Done()
			}
		}(pair)
	}
	wg.Wait()
	logrus.Infof("[%s] RepublishPairList successfully.", node.ip)
}

func (node *Node) RepublishPair(pair Pair, done chan bool) {
	logrus.Infof("[%s] RepublishPair begins. pair: %s", node.ip, pair)
	nodeList := node.NodeLookup(hashString(pair.Key))
	var wg sync.WaitGroup
	wg.Add(len(nodeList))
	for _, ip := range nodeList {
		go func(ip string) {
			defer wg.Done()
			if ip == node.ip {
				err := node.Store(&pair, nil)
				if err != nil {
				}
			} else {
				err := node.RemoteCall(ip, "Node.Store", &pair, nil)
				if err != nil {
					logrus.Errorf("[%s] RepublishPair. Node.Store failed. ip: %s", node.ip, ip)
				}
			}
		}(ip)
	}
	wg.Wait()
	done <- true
	logrus.Infof("[%s] RepublishPair successfully.", node.ip)
}
func (node *Node) Refresh() {
	refreshList := node.rt.GetRefreshList()
	var wg sync.WaitGroup
	wg.Add(len(refreshList))
	for _, i := range refreshList {
		go func(i int) {
			node.NodeLookup(new(big.Int).Xor(pow[i], node.id))
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (node *Node) Maintain() {
	go func() {
		for node.online {
			pairs := node.db.GetRepublishList()
			node.RepublishPairList(pairs)
			time.Sleep(RepublishCycleTime)
		}
	}()

	go func() {
		for node.online {
			node.db.CheckExpire()
			time.Sleep(ExpireCycleTime)
		}
	}()

	go func() {
		for node.online {
			node.Refresh()
			time.Sleep(RefreshCycleTime)
		}
	}()
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
	node.listener, err = net.Listen("tcp", node.ip)
	node.onlineLock.Lock()
	node.online = true
	node.onlineLock.Unlock()
	go func() {
		for node.online {
			conn, err := node.listener.Accept()
			if err != nil {
				return
			}
			go node.server.ServeConn(conn)
		}
	}()
}
func (node *Node) Create() {
	logrus.Infof("Welcome to Kademlia test! This is stupid debugger CrazyDave. Have fun!")
	node.Maintain()
}

func (node *Node) Join(ip string) bool {
	logrus.Infof("[%s] What's up, dude! I'm joining, you know? ip: %s", node.ip, ip)
	ind := node.WhichBucket(hashString(ip))
	if node.rt.buckets[ind].Size() < K {
		node.rt.buckets[ind].PushFront(ip)
	}
	node.NodeLookup(node.id)
	node.Maintain()
	logrus.Infof("[%s] Join successfully. ip: %s", node.ip, ip)
	return true
}
func (node *Node) Quit() {
	logrus.Infof("[%s] Quit begins", node.ip)
	defer logrus.Infof("[%s] Quit successfully.", node.ip)
	if !node.online {
		return
	}
	republishList := node.db.GetAll()
	node.RepublishPairList(republishList)
	err := node.listener.Close()
	if err != nil {
	}
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
}
func (node *Node) ForceQuit() {
	if !node.online {
		return
	}
	err := node.listener.Close()
	if err != nil {
	}
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
}
func (node *Node) Put(key string, value string) (flag bool) {
	logrus.Infof("[%s] Put begins. key: %s, value: %s", node.ip, key[len(key)-5:], value[len(value)-5:])
	logrus.Infof("[%s] Put. NodeLookup begins.", node.ip)
	NodeList := node.NodeLookup(hashString(key))
	logrus.Infof("[%s] Put. NodeLookup ends. NodeList: %s", node.ip, NodeList)
	flag = false
	var flagLock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(NodeList))
	for _, ip := range NodeList {
		go func(ip string) {
			defer wg.Done()
			if node.ip == ip {
				err := node.Store(&Pair{key, value}, nil)
				if err != nil {
					logrus.Errorf("[%s] Put failed. Store failed. ip: %s", node.ip, ip)
				} else {
					flagLock.Lock()
					flag = true
					flagLock.Unlock()
				}
			} else {
				err := node.RemoteCall(ip, "Node.Store", &Pair{key, value}, nil)
				if err != nil {
					logrus.Errorf("[%s] Put failed. Node.Store failed. ip: %s", node.ip, ip)
				} else {
					flagLock.Lock()
					flag = true
					flagLock.Unlock()
				}
			}
		}(ip)
	}
	wg.Wait()
	if flag {
		logrus.Infof("[%s] Put successfully. key: %s, value: %s", node.ip, key[len(key)-5:], value[len(value)-5:])
	} else {
		logrus.Infof("[%s] Put failed. key: %s, value: %s", node.ip, key[len(key)-5:], value[len(value)-5:])
	}
	return
}

// Get is actually the FindValue procedure
func (node *Node) Get(key string) (ok bool, v string) {
	logrus.Infof("[%s] Get begins. key: %s", node.ip, key[len(key)-5:])
	var sl ShortList
	id := hashString(key)
	sl.Init(id)
	findList := make([]string, 0)
	logrus.Infof("[%s] Get. FindNode begins.", node.ip)
	err := node.FindNode(id, &findList)
	logrus.Infof("[%s] FindNode ends. findList: %s", node.ip, findList)
	if err != nil {
		return
	}
	for _, v := range findList {
		sl.Insert(v)
	}
	for {
		queryList := sl.GetAlphaNotQueried()
		findList = make([]string, 0)
		ok, v = node.MakeFetch(key, &queryList, &findList, &sl)
		if ok {
			logrus.Infof("[%s] Get successfully.", node.ip)
			return
		}
		changed := sl.Update(findList)
		if !changed {
			queryList = sl.GetAllNotQueried()
			findList = make([]string, 0)
			ok, v = node.MakeFetch(key, &queryList, &findList, &sl)
			if ok {
				logrus.Infof("[%s] Get successfully.", node.ip)
				return
			}
			changed = sl.Update(findList)
			if !changed {
				logrus.Infof("[%s] Get failed. Not found.", node.ip)
				return false, ""
			}
		}
	}
}
func (node *Node) Delete(key string) bool {
	return true
}
