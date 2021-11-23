package mvcc

import (
	"fmt"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"math"
	"strconv"
	"sync"
	"time"
)

const (
	// lfuSample is the number of items to sample when looking at eviction
	// candidates. 5 seems to be the most optimal number.
	lfuSample = 5
	// default estimator counter size
	defaultEstimatorCapacity = 100
	// default cache cost upper limit
	defaultCostCapacity = 100 * 1024 * 1024
	//costThreshold       = 1024 * 1024 * 1024
)

type storeCache struct {
	lock          *sync.RWMutex
	costEstimator costEstimator
	cache         map[revision]*mvccpb.KeyValue
	currentCost   int
	costCapacity  int
	lastUpdate    time.Time
	cachedReq     string
}

type costEstimator struct {
	lock       *sync.Mutex
	bound      int
	counter    map[string]int
	maxCostReq string
}

func newStoreCache() *storeCache {
	c := new(storeCache)
	c.costEstimator = costEstimator{
		lock:    new(sync.Mutex),
		bound:   defaultEstimatorCapacity,
		counter: make(map[string]int),
	}
	c.cache = make(map[revision]*mvccpb.KeyValue)
	c.lock = new(sync.RWMutex)
	c.costCapacity = defaultCostCapacity
	c.lastUpdate = time.Time{}
	return c
}

func (c *storeCache) get(key revision) (*mvccpb.KeyValue, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	e, ok := c.cache[key]
	return e, ok
}

func (c *storeCache) set(key revision, value *mvccpb.KeyValue, cost int) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.cache[key]; !ok {
		return c.unsafeSet(key, value, cost)
	}
	return true
}

func (c *storeCache) unsafeSet(key revision, value *mvccpb.KeyValue, cost int) bool {
	if c.currentCost < c.costCapacity-cost {
		c.cache[key] = value
		c.currentCost += cost
		//fmt.Println("capacity success")
		//fmt.Println(c.currentCost)
		//fmt.Println(cost)
		return true
	} else {
		fmt.Println("capacity full, can't cache")
		fmt.Println(c.currentCost)
		fmt.Println(cost)
		return false
	}
}

func (c *storeCache) hasCachedRequest(request string) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cachedReq == request
}

func (c *storeCache) updateCacheIfNecessary(request string, cost int, rvs []revision, kvs []*mvccpb.KeyValue) {
	if c.costEstimator.updateRequestCosts(request, cost) {
		c.lock.Lock()
		defer c.lock.Unlock()
		if time.Since(c.lastUpdate) >= 1*time.Minute && request != c.cachedReq {
			c.clear()
			for i, r := range rvs {
				fmt.Println("caching item " + strconv.Itoa(i))
				if !c.unsafeSet(r, kvs[i], kvs[i].Size()) {
					break
				}
			}
			c.cachedReq = request
			fmt.Println("caching updated " + strconv.Itoa(c.currentCost))
		}
	}
}

func (c *storeCache) clear() {
	c.cache = make(map[revision]*mvccpb.KeyValue)
	c.currentCost = 0
	c.cachedReq = ""
	c.lastUpdate = time.Now()
	c.costEstimator.clear()
}

func (c *costEstimator) updateRequestCosts(request string, cost int) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if co, ok := c.counter[request]; ok {
		c.counter[request] = co + cost
	} else {
		if len(c.counter) > c.bound {
			c.evict()
		}
		c.counter[request] = cost
	}
	if c.counter[request] > c.counter[c.maxCostReq] {
		c.maxCostReq = request
		return true
	}
	return false
}

func (c *costEstimator) evict() {
	minCost := math.MaxInt16
	minCostReq := ""
	i := 0
	for req, cost := range c.counter {
		if cost < minCost {
			minCost = cost
			minCostReq = req
		}
		i++
		if i == lfuSample {
			break
		}
	}
	delete(c.counter, minCostReq)
	return
}

func (c *costEstimator) clear() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.counter = make(map[string]int)
}

// can be used to optimize to reduce counter periodically like tiny lfu
func (c *costEstimator) reset() {
}
