package Kademlia

import (
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
	rand.Seed(time.Now().UnixNano())
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
func (node *Node) RemoteCall(ip string, method string, args interface{}, reply interface{}, notify bool) error {
	err := RemoteCall(ip, method, args, reply)
	if err == nil && notify {
		err = RemoteCall(ip, "Node.Notify", node.ip, nil)
	}
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

// Notify 函数告知对方自己的存在，使得对方的路由表能得以更新
func (node *Node) Notify(senderIp string, _ *struct{}) error {
	node.Update(senderIp, true)
	return nil
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
		//logrus.Infof("[%s] Update. ind: %v, ip: %s", node.ip, ind, ip)
	}
}

// FindNode 返回自己路由表中距离id最近的k个节点ip
func (node *Node) FindNode(id *big.Int, reply *[]string) error {
	//var done chan bool
	done := make(chan bool, 1)
	go func() {
		ind := node.WhichBucket(id)
		if ind == -1 {
			*reply = append(*reply, node.ip)
		} else {
			buc := node.rt.GetNodes(ind)
			*reply = append(*reply, buc...)
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
		done <- true
	}()
	select {
	case <-done:
		return nil
	case <-time.After(FindNodeTimeout):
		logrus.Errorf("[%s]  FindNode. Timeout.", node.ip)
		return nil
	}
}

// NodeLookup 返回距离id最近的k个ip
func (node *Node) NodeLookup(id *big.Int, notify bool) (kClosest []string) {
	var sl ShortList
	sl.Init(id)
	done := make(chan bool, 1)
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
			node.MakeQuery(id, &queryList, &findList, &sl, notify)
			changed := sl.Update(findList)
			if !changed {
				queryList = sl.GetAllNotQueried()
				findList = make([]string, 0)
				node.MakeQuery(id, &queryList, &findList, &sl, notify)
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
		logrus.Errorf("[%s] NodeLookup. Timeout.", node.ip)
		return sl.GetKClosest()
	}
}

func (node *Node) MakeQuery(id *big.Int, queryList *[]string, findList *[]string, sl *ShortList, notify bool) {
	var wg sync.WaitGroup
	wg.Add(len(*queryList))
	findListLock := sync.Mutex{}
	for _, ip := range *queryList {
		go func(ip string) {
			defer wg.Done()
			reply := make([]string, 0)
			err := node.RemoteCall(ip, "Node.FindNode", id, &reply, notify)
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

func (node *Node) MakeFetch(key string, queryList *[]string, findList *[]string, sl *ShortList, notify bool) (ok bool, v string) {
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
			err := node.RemoteCall(ip, "Node.FindNode", id, &reply, notify)
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

	done := make(chan bool, 1)
	go func() {
		var wg sync.WaitGroup
		wg.Add(len(pairs))
		for _, pair := range pairs {
			go func(pair Pair) {
				prDone := make(chan bool, 1)
				go node.RepublishPair(pair, prDone)
				select {
				case <-prDone:
					wg.Done()
				case <-time.After(RepublishPairTimeOut):
					wg.Done()
				}
			}(pair)
		}
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		logrus.Infof("[%s] RepublishPairList successfully.", node.ip)
	case <-time.After(RepublishTimeOut):
		logrus.Errorf("[%s] RepublishPairList failed. Timeout.", node.ip)
	}

}

func (node *Node) RepublishPair(pair Pair, done chan bool) {
	//logrus.Infof("[%s] RepublishPair begins. pair: %s", node.ip, pair)
	nodeList := node.NodeLookup(hashString(pair.Key), false)
	var wg sync.WaitGroup
	wg.Add(len(nodeList))
	for _, ip := range nodeList {
		go func(ip string) {
			defer wg.Done()
			if ip == node.ip {
				err := node.Store(&pair, nil)
				if err != nil {
					logrus.Errorf("[%s] RepublishPair. Store failed.", node.ip)
				} else {
					//logrus.Errorf("[%s] RepublishPair. Store successfully.", node.ip)
				}
			} else {
				err := node.RemoteCall(ip, "Node.Store", &pair, nil, true)
				if err != nil {
					logrus.Errorf("[%s] RepublishPair. Node.Store failed. ip: %s", node.ip, ip)
				} else {
					//logrus.Infof("[%s] RepublishPair. Node.Store successfully. ip: %s", node.ip, ip)
				}
			}
		}(ip)
	}
	wg.Wait()
	done <- true
	//logrus.Infof("[%s] RepublishPair ends.", node.ip)
}
func (node *Node) Refresh() {
	refreshList := node.rt.GetRefreshList()
	if len(refreshList) > 0 {
		randIndex := rand.Intn(len(refreshList))
		refreshIndex := refreshList[randIndex]
		//logrus.Infof("[%s] Refresh begins. refreshList: %v", node.ip, refreshList)
		node.NodeLookup(new(big.Int).Xor(pow[refreshIndex], node.id), false)
		//logrus.Infof("[%s] Refresh ends", node.ip)
	}
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

	//go func() {
	//	for node.online {
	//		allNodes := make([]string, 0)
	//		for _, buc := range node.rt.buckets {
	//			buc.nodesLock.RLock()
	//			for e := buc.nodes.Front(); e != nil; e = e.Next() {
	//				allNodes = append(allNodes, e.Value.(string))
	//			}
	//			buc.nodesLock.RUnlock()
	//		}
	//		logrus.Infof("[%s] Printing the whole routing table: %s", node.ip, allNodes)
	//		time.Sleep(2 * time.Second)
	//	}
	//}()
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
	logrus.Infof("[%s] Join. NodeLookup begins.", node.ip)
	NodeList := node.NodeLookup(node.id, true)
	logrus.Infof("[%s] Join. NodeLookup ends. NodeList: %v", node.ip, NodeList)
	node.Maintain()
	logrus.Infof("[%s] Join successfully. ip: %s", node.ip, ip)
	return true
}
func (node *Node) Quit() {
	logrus.Infof("[%s] Quit begins", node.ip)
	if !node.online {
		return
	}
	republishList := node.db.GetAll()
	logrus.Infof("[%s] Quit. RepublishPairList begins. republishList: %s", node.ip, republishList)
	node.RepublishPairList(republishList)
	logrus.Infof("[%s] Quit. RepublishPairList ends. republishList: %s", node.ip, republishList)
	err := node.listener.Close()
	if err != nil {
	}
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	logrus.Infof("[%s] Quit successfully.", node.ip)
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
	NodeList := node.NodeLookup(hashString(key), true)
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
					logrus.Errorf("[%s] Put. Store failed. ip: %s", node.ip, ip)
				} else {
					logrus.Errorf("[%s] Put. Store successfully. ip: %s", node.ip, ip)
					flagLock.Lock()
					flag = true
					flagLock.Unlock()
				}
			} else {
				err := node.RemoteCall(ip, "Node.Store", &Pair{key, value}, nil, true)
				if err != nil {
					logrus.Errorf("[%s] Put. Node.Store failed. ip: %s", node.ip, ip)
				} else {
					logrus.Errorf("[%s] Put. Node.Store successfully. ip: %s", node.ip, ip)
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
	for _, ip := range findList {
		sl.Insert(ip)
	}
	for {
		queryList := sl.GetAlphaNotQueried()
		findList = make([]string, 0)
		ok, v = node.MakeFetch(key, &queryList, &findList, &sl, true)
		if ok {
			logrus.Infof("[%s] Get successfully.", node.ip)
			return
		}
		changed := sl.Update(findList)
		if !changed {
			queryList = sl.GetAllNotQueried()
			findList = make([]string, 0)
			ok, v = node.MakeFetch(key, &queryList, &findList, &sl, true)
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
