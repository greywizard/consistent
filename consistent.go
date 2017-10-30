// An implementation of Consistent Hashing and
// Consistent Hashing With Bounded Loads.
//
// https://en.wikipedia.org/wiki/Consistent_hashing
//
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
package consistent

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/OneOfOne/xxhash"
)

var ErrNoHosts = errors.New("no hosts added")

type Host struct {
	Name string
}

type Consistent struct {
	hosts             map[uint64]string
	sortedSet         []uint64
	replicationFactor int

	sync.RWMutex
}

func New(factor int) *Consistent {
	return &Consistent{
		hosts:             map[uint64]string{},
		sortedSet:         []uint64{},
		replicationFactor: factor,
	}
}

func (c *Consistent) Add(host string) {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.replicationFactor; i++ {
		h := c.hash(fmt.Sprintf("%s%d", host, i))
		c.hosts[h] = host
		c.sortedSet = append(c.sortedSet, h)

	}
	// sort hashes ascendingly
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		if c.sortedSet[i] < c.sortedSet[j] {
			return true
		}
		return false
	})
}

// Returns the host that owns `key`.
//
// As described in https://en.wikipedia.org/wiki/Consistent_hashing
//
// It returns ErrNoHosts if the ring has no hosts in it.
func (c *Consistent) Get(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.hosts) == 0 {
		return "", ErrNoHosts
	}

	h := c.hash(key)
	idx := c.search(h)
	return c.hosts[c.sortedSet[idx]], nil
}

func (c *Consistent) search(key uint64) int {
	idx := sort.Search(len(c.sortedSet), func(i int) bool {
		return c.sortedSet[i] >= key
	})

	if idx >= len(c.sortedSet) {
		idx = 0
	}
	return idx
}

// Deletes host from the ring
func (c *Consistent) Remove(host string) bool {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.replicationFactor; i++ {
		h := c.hash(fmt.Sprintf("%s%d", host, i))
		delete(c.hosts, h)
		c.delSlice(h)
	}
	return true
}

// Return the list of hosts in the ring
func (c *Consistent) Hosts() (hosts []string) {
	c.RLock()
	defer c.RUnlock()
	for _, host := range c.hosts {
		hosts = append(hosts, host)
	}
	return hosts
}

func (c *Consistent) delSlice(val uint64) {
	for i := 0; i < len(c.sortedSet); i++ {
		if c.sortedSet[i] == val {
			c.sortedSet = append(c.sortedSet[:i], c.sortedSet[i+1:]...)
		}
	}
}

func (c *Consistent) hash(key string) uint64 {
	h := xxhash.New64()
	h.Write([]byte(key))
	return h.Sum64()

}
