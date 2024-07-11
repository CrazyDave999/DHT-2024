package Kademlia

import (
	"container/list"
	"crypto/sha1"
	"github.com/sirupsen/logrus"
	"math/big"
	"sync"
	"time"
)

const (
	M                    int = 160
	K                    int = 10
	A                    int = 3 // The system-wide concurrency parameter alpha.
	PingTime                 = 200 * time.Millisecond
	RepublishCycleTime       = 15 * time.Second
	RepublishPairTimeOut     = 250 * time.Millisecond
	RepublishTimeOut         = 15 * time.Second
	ExpireCycleTime          = 60 * time.Second
	RefreshCycleTime         = 3 * time.Second
	RefreshTime              = 15 * time.Second
	NodeLookupTimeout        = 5 * time.Second
	FindNodeTimeout          = 250 * time.Millisecond
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
	logrus.Infof("CheckExpire. keys: %s", keys)
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
	ip  string
	dis *big.Int
}
type ShortList struct {
	nodes     *list.List
	nodesLock sync.RWMutex
	targetId  *big.Int
	queried   map[string]bool
}

func (sl *ShortList) Init(id *big.Int) {
	sl.nodes = list.New()
	sl.targetId = new(big.Int).Set(id)
	sl.queried = make(map[string]bool)
}

func (sl *ShortList) SetQueried(ip string) {
	sl.nodesLock.Lock()
	defer sl.nodesLock.Unlock()
	sl.queried[ip] = true
}
func (sl *ShortList) Size() int {
	return sl.nodes.Len()
}
func (sl *ShortList) Find(ip string) *list.Element {
	sl.nodesLock.RLock()
	defer sl.nodesLock.RUnlock()
	for e := sl.nodes.Front(); e != nil; e = e.Next() {
		if e.Value.(ShortListNode).ip == ip {
			return e
		}
	}
	return nil
}
func (sl *ShortList) Insert(ip string) int {
	if sl.Find(ip) != nil {
		return -1
	}
	node := ShortListNode{
		ip,
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
	sl.queried[ip] = false
	sl.nodesLock.Unlock()
	return i
}
func (sl *ShortList) Remove(ip string) {
	sl.nodesLock.Lock()
	defer sl.nodesLock.Unlock()
	for e := sl.nodes.Front(); e != nil; e = e.Next() {
		if e.Value.(ShortListNode).ip == ip {
			sl.nodes.Remove(e)
			delete(sl.queried, ip)
			break
		}
	}
}

func (sl *ShortList) GetAlphaNotQueried() (res []string) {
	sl.nodesLock.RLock()
	defer sl.nodesLock.RUnlock()
	i := 0
	for e := sl.nodes.Front(); e != nil && i < K; e = e.Next() {
		node := e.Value.(ShortListNode)
		if !sl.queried[node.ip] {
			res = append(res, node.ip)
		}
		if len(res) == A {
			return
		}
		i++
	}
	return
}
func (sl *ShortList) GetAllNotQueried() (res []string) {
	sl.nodesLock.RLock()
	defer sl.nodesLock.RUnlock()
	i := 0
	for e := sl.nodes.Front(); e != nil && i < K; e = e.Next() {
		node := e.Value.(ShortListNode)
		if !sl.queried[node.ip] {
			res = append(res, node.ip)
		}
		i++
	}
	return
}

func (sl *ShortList) Update(findList []string) (changed bool) {
	changed = false
	for _, ip := range findList {
		ind := sl.Insert(ip)
		if ind != -1 && ind < K {
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
