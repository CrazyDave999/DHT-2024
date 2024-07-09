package Kademlia

import (
	"container/list"
	"crypto/sha1"
	"math/big"
	"sync"
	"time"
)

const (
	M                    int = 160
	K                    int = 8
	A                    int = 3 // The system-wide concurrency parameter alpha.
	PingTime                 = 100 * time.Millisecond
	RepublishCycleTime       = 15 * time.Second
	RepublishPairTimeOut     = 50 * time.Millisecond
	RepublishTimeOut         = 15 * time.Second
	ExpireCycleTime          = 60 * time.Second
	RefreshCycleTime         = 3 * time.Second
	RefreshTime              = 15 * time.Second
)

func hashString(s string) *big.Int {
	h := sha1.New()
	h.Write([]byte(s))
	hashed := h.Sum(nil)
	return new(big.Int).SetBytes(hashed)
}

type Pair struct {
	Key   string
	Value string
}
type DataBase struct {
	data          map[string]string
	dataLock      sync.RWMutex
	NextRepublish map[string]time.Time
	NextExpire    map[string]time.Time
}

func (db *DataBase) Init() {
	db.dataLock.Lock()
	db.data = make(map[string]string)
	db.NextRepublish = make(map[string]time.Time)
	db.NextExpire = make(map[string]time.Time)
	db.dataLock.Unlock()
}
func (db *DataBase) Put(key string, value string) {
	db.dataLock.Lock()
	db.data[key] = value
	db.NextRepublish[key] = time.Now().Add(RepublishCycleTime)
	db.NextExpire[key] = time.Now().Add(ExpireCycleTime)
	db.dataLock.Unlock()
}

func (db *DataBase) Get(key string) (v string, ok bool) {
	db.dataLock.RLock()
	v, ok = db.data[key]
	db.dataLock.RUnlock()
	return
}

func (db *DataBase) CheckExpire() {
	var keys []string
	db.dataLock.Lock()
	for k := range db.data {
		if time.Now().After(db.NextExpire[k]) {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		delete(db.data, k)
		delete(db.NextExpire, k)
		delete(db.NextRepublish, k)
	}
	db.dataLock.Unlock()
}
func (db *DataBase) GetRepublishList() (res []Pair) {
	db.dataLock.RLock()
	for k, v := range db.data {
		if time.Now().After(db.NextRepublish[k]) {
			res = append(res, Pair{k, v})
		}
	}
	db.dataLock.RUnlock()
	return
}
func (db *DataBase) GetAll() (res []Pair) {
	db.dataLock.RLock()
	for k, v := range db.data {
		res = append(res, Pair{k, v})
	}
	db.dataLock.RUnlock()
	return
}

// a fake priority_queue for NodeLookup procedure

type ShortListNode struct {
	ip      string
	queried bool
	dis     *big.Int
}
type ShortList struct {
	nodes     *list.List
	nodesLock sync.RWMutex
	targetId  *big.Int
}

func (sl *ShortList) Init(id *big.Int) {
	sl.nodes = list.New()
	sl.targetId = new(big.Int).Set(id)
}
func (sl *ShortList) SetQueried(e *list.Element) {
	slNode := e.Value.(ShortListNode)
	e.Value = ShortListNode{
		ip:      slNode.ip,
		queried: true,
		dis:     slNode.dis,
	}
}
func (sl *ShortList) Size() int {
	return sl.nodes.Len()
}
func (sl *ShortList) Insert(ip string) int {
	node := ShortListNode{
		ip,
		false,
		new(big.Int).Xor(hashString(ip), sl.targetId),
	}
	sl.nodesLock.RLock()
	e := sl.nodes.Front()
	i := 0
	for ; e != nil; e = e.Next() {
		if e.Value.(ShortListNode).dis.Cmp(node.dis) > 0 {
			break
		}
		i++
	}
	sl.nodesLock.RUnlock()
	sl.nodesLock.Lock()
	if e == nil {
		sl.nodes.PushBack(node)
	} else {
		sl.nodes.InsertBefore(node, e)
	}

	sl.nodesLock.Unlock()
	return i
}
func (sl *ShortList) Remove(e *list.Element) {
	sl.nodesLock.Lock()
	defer sl.nodesLock.Unlock()
	sl.nodes.Remove(e)
}

func (sl *ShortList) GetAlphaNotQueried() (res []*list.Element) {
	sl.nodesLock.RLock()
	defer sl.nodesLock.RUnlock()
	for e := sl.nodes.Front(); e != nil; e = e.Next() {
		node := e.Value.(ShortListNode)
		if !node.queried {
			res = append(res, e)
		}
		if len(res) == A {
			return
		}
	}
	return
}
func (sl *ShortList) GetAllNotQueried() (res []*list.Element) {
	sl.nodesLock.RLock()
	defer sl.nodesLock.RUnlock()
	for e := sl.nodes.Front(); e != nil; e = e.Next() {
		node := e.Value.(ShortListNode)
		if !node.queried {
			res = append(res, e)
		}
	}
	return
}

func (sl *ShortList) Update(findList []string) (changed bool) {
	changed = false
	for _, ip := range findList {
		if sl.Insert(ip) < K {
			changed = true
		}
	}
	return
}

func (sl *ShortList) GetKClosest() (kClosest []string) {
	sl.nodesLock.RLock()
	defer sl.nodesLock.RUnlock()
	i := 0
	for e := sl.nodes.Front(); e != nil && i < K; e = e.Next() {
		node := e.Value.(ShortListNode)
		kClosest = append(kClosest, node.ip)
		i++
	}
	return
}
