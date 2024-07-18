package Kademlia

import (
	"container/list"
	"sync"
	"time"
)

type Bucket struct {
	nodes       *list.List
	nodesLock   sync.RWMutex
	lastRefresh time.Time
}

func NewBucket() *Bucket {
	return &Bucket{
		nodes:       list.New(),
		lastRefresh: time.Now(),
	}
}

func (buc *Bucket) Update(ip string, online bool) {
	buc.lastRefresh = time.Now()
	buc.nodesLock.Lock()
	e := buc.nodes.Front()
	for ; e != nil; e = e.Next() {
		if e.Value.(string) == ip {
			break
		}
	}

	if online {
		if e != nil {

			buc.nodes.MoveToFront(e)

		} else {

			size := buc.nodes.Len()

			if size < K {

				buc.nodes.PushFront(ip)

			} else {

				backIp := buc.nodes.Back().Value.(string)

				if !TryPing(backIp) {

					buc.nodes.Remove(buc.nodes.Back())
					buc.nodes.PushFront(ip)

				} else {

					buc.nodes.MoveToFront(buc.nodes.Back())

				}

			}
		}
	} else {
		if e != nil {

			buc.nodes.Remove(e)

		}
	}
	buc.nodesLock.Unlock()
}

func (buc *Bucket) PushFront(ip string) {
	buc.nodesLock.Lock()
	buc.nodes.PushFront(ip)
	buc.nodesLock.Unlock()
}
func (buc *Bucket) Size() int {
	buc.nodesLock.RLock()
	defer buc.nodesLock.RUnlock()
	return buc.nodes.Len()
}

type RoutingTable struct {
	buckets [M]*Bucket
}

func NewRoutingTable() *RoutingTable {
	rt := &RoutingTable{}
	for i := 0; i < M; i++ {
		rt.buckets[i] = NewBucket()
	}
	return rt
}

func (rt *RoutingTable) GetNodes(ind int) (res []string) {
	rt.buckets[ind].nodesLock.RLock()
	for e := rt.buckets[ind].nodes.Front(); e != nil; e = e.Next() {
		res = append(res, e.Value.(string))
	}
	rt.buckets[ind].nodesLock.RUnlock()
	return
}
func (rt *RoutingTable) Update(ind int, ip string, online bool) {
	rt.buckets[ind].Update(ip, online)
}

func (rt *RoutingTable) GetRefreshList() (res []int) {
	t := time.Now()
	for i := 150; i < M; i++ {
		if t.After(rt.buckets[i].lastRefresh.Add(RefreshTime)) {
			res = append(res, i)
		}
	}
	return
}
