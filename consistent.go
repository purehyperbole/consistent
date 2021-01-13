// Copyright (c) 2018 Burak Sezer
// All rights reserved.
//
// This code is licensed under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package consistent provides a consistent hashing function with bounded loads.
// For more information about the underlying algorithm, please take a look at
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
//
// Example Use:
// 	cfg := consistent.Config{
// 		PartitionCount:    71,
// 		ReplicationFactor: 20,
// 		Load:              1.25,
// 		Hasher:            hasher{},
//	}
//
//      // Create a new consistent object
//      // You may call this with a list of members
//      // instead of adding them one by one.
//	c := consistent.New(members, cfg)
//
//      // myMember struct just needs to implement a String method.
//      // New/Add/Remove distributes partitions among members using the algorithm
//      // defined on Google Research Blog.
//	c.Add(myMember)
//
//	key := []byte("my-key")
//      // LocateKey hashes the key and calculates partition ID with
//      // this modulo operation: MOD(hash result, partition count)
//      // The owner of the partition is already calculated by New/Add/Remove.
//      // LocateKey just returns the member which's responsible for the key.
//	member := c.LocateKey(key)
//
package consistent

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	//ErrInsufficientMemberCount represents an error which means there are not enough members to complete the task.
	ErrInsufficientMemberCount = errors.New("insufficient member count")

	// ErrMemberNotFound represents an error which means requested member could not be found in consistent hash ring.
	ErrMemberNotFound = errors.New("member could not be found in ring")
)

// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
// Hasher should minimize collisions (generating same hash for different byte slice)
// and while performance is also important fast functions are preferable (i.e.
// you can use FarmHash family).
type Hasher interface {
	Sum64([]byte) uint64
}

// Member interface represents a member in consistent hash ring.
type Member interface {
	String() string
}

// Config represents a structure to control consistent package.
type Config struct {
	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	PartitionCount int

	// Members are replicated on consistent hash ring. This number means that a member
	// how many times replicated on the ring.
	ReplicationFactor int

	// Load is used to calculate average load. See the code, the paper and Google's blog post to learn about it.
	Load float64
}

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	mu sync.RWMutex

	state *unsafe.Pointer
}

// New creates and returns a new Consistent object.
func New(members []Member, config Config) *Consistent {
	if config.Hasher == nil {
		panic("Hasher cannot be nil")
	}

	s := unsafe.Pointer(&state{
		config:         config,
		hasher:         config.Hasher,
		members:        make(map[string]*Member),
		partitionCount: uint64(config.PartitionCount),
		ring:           make(map[uint64]*Member),
	})

	c := &Consistent{
		state: &s,
	}

	for _, member := range members {
		c.getState().add(member)
	}
	if members != nil {
		c.getState().distributePartitions()
	}
	return c
}

// GetMembers returns a thread-safe copy of members.
func (c *Consistent) GetMembers() []Member {
	s := c.getState()

	// Create a thread-safe copy of member list.
	members := make([]Member, 0, len(s.members))
	for _, member := range s.members {
		members = append(members, *member)
	}
	return members
}

// AverageLoad exposes the current average load.
func (c *Consistent) AverageLoad() float64 {
	return c.getState().averageLoad()
}

// Add adds a new member to the consistent hash circle.
func (c *Consistent) Add(member Member) {
	for {
		s := c.getState()

		if _, ok := s.members[member.String()]; ok {
			// We already have this member. Quit immediately.
			return
		}

		ns := s.copy()

		ns.add(member)
		ns.distributePartitions()

		if atomic.CompareAndSwapPointer(c.state, unsafe.Pointer(s), unsafe.Pointer(ns)) {
			return
		}
	}
}

// Remove removes a member from the consistent hash circle.
func (c *Consistent) Remove(name string) {
	for {
		s := c.getState()

		if _, ok := s.members[name]; !ok {
			// There is no member with that name. Quit immediately.
			return
		}

		ns := s.copy()

		for i := 0; i < ns.config.ReplicationFactor; i++ {
			key := []byte(fmt.Sprintf("%s%d", name, i))
			h := ns.hasher.Sum64(key)
			delete(ns.ring, h)
			ns.delSlice(h)
		}
		delete(ns.members, name)
		if len(ns.members) == 0 {
			// consistent hash ring is empty now. Reset the partition table.
			ns.partitions = make(map[int]*Member)
			if atomic.CompareAndSwapPointer(c.state, unsafe.Pointer(s), unsafe.Pointer(ns)) {
				return
			} else {
				continue
			}
		}
		ns.distributePartitions()

		if atomic.CompareAndSwapPointer(c.state, unsafe.Pointer(s), unsafe.Pointer(ns)) {
			return
		}
	}
}

// LoadDistribution exposes load distribution of members.
func (c *Consistent) LoadDistribution() map[string]float64 {
	return c.getState().loads
}

// FindPartitionID returns partition id for given key.
func (c *Consistent) FindPartitionID(key []byte) int {
	s := c.getState()
	hkey := s.hasher.Sum64(key)
	return int(hkey % s.partitionCount)
}

// GetPartitionOwner returns the owner of the given partition.
func (c *Consistent) GetPartitionOwner(partID int) Member {
	s := c.getState()

	member, ok := s.partitions[partID]
	if !ok {
		return nil
	}
	// Create a thread-safe copy of member and return it.
	return *member
}

// LocateKey finds a home for given key
func (c *Consistent) LocateKey(key []byte) Member {
	partID := c.FindPartitionID(key)
	return c.GetPartitionOwner(partID)
}

// GetClosestN returns the closest N member to a key in the hash ring.
// This may be useful to find members for replication.
func (c *Consistent) GetClosestN(key []byte, count int) ([]Member, error) {
	partID := c.FindPartitionID(key)
	return c.getState().getClosestN(partID, count)
}

// GetClosestNForPartition returns the closest N member for given partition.
// This may be useful to find members for replication.
func (c *Consistent) GetClosestNForPartition(partID, count int) ([]Member, error) {
	return c.getState().getClosestN(partID, count)
}

func (c *Consistent) getState() *state {
	return (*state)(atomic.LoadPointer(c.state))
}

type state struct {
	config         Config
	hasher         Hasher
	sortedSet      []uint64
	partitionCount uint64
	loads          map[string]float64
	members        map[string]*Member
	partitions     map[int]*Member
	ring           map[uint64]*Member
}

func (s *state) averageLoad() float64 {
	avgLoad := float64(s.partitionCount/uint64(len(s.members))) * s.config.Load
	return math.Ceil(avgLoad)
}

func (s *state) distributeWithLoad(partID, idx int, partitions map[int]*Member, loads map[string]float64) {
	avgLoad := s.averageLoad()
	var count int
	for {
		count++
		if count >= len(s.sortedSet) {
			// User needs to decrease partition count, increase member count or increase load factor.
			panic("not enough room to distribute partitions")
		}
		i := s.sortedSet[idx]
		member := *s.ring[i]
		load := loads[member.String()]
		if load+1 <= avgLoad {
			partitions[partID] = &member
			loads[member.String()]++
			return
		}
		idx++
		if idx >= len(s.sortedSet) {
			idx = 0
		}
	}
}

func (s *state) distributePartitions() {
	loads := make(map[string]float64)
	partitions := make(map[int]*Member)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < s.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := s.hasher.Sum64(bs)
		idx := sort.Search(len(s.sortedSet), func(i int) bool {
			return s.sortedSet[i] >= key
		})
		if idx >= len(s.sortedSet) {
			idx = 0
		}
		s.distributeWithLoad(int(partID), idx, partitions, loads)
	}
	s.partitions = partitions
	s.loads = loads
}

func (s *state) add(member Member) {
	for i := 0; i < s.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.String(), i))
		h := s.hasher.Sum64(key)
		s.ring[h] = &member
		s.sortedSet = append(s.sortedSet, h)
	}
	// sort hashes ascendingly
	sort.Slice(s.sortedSet, func(i int, j int) bool {
		return s.sortedSet[i] < s.sortedSet[j]
	})
	// Storing member at this map is useful to find backup members of a partition.
	s.members[member.String()] = &member
}

func (s *state) delSlice(val uint64) {
	for i := 0; i < len(s.sortedSet); i++ {
		if s.sortedSet[i] == val {
			s.sortedSet = append(s.sortedSet[:i], s.sortedSet[i+1:]...)
			break
		}
	}
}

func (s *state) getClosestN(partID, count int) ([]Member, error) {
	res := []Member{}
	if count > len(s.members) {
		return res, ErrInsufficientMemberCount
	}

	var ownerKey uint64
	owner, ok := s.partitions[partID]
	if !ok {
		return nil, errors.New("unknown partition owner")
	}
	// Hash and sort all the names.
	keys := []uint64{}
	kmems := make(map[uint64]*Member)
	for name, member := range s.members {
		key := s.hasher.Sum64([]byte(name))
		if name == (*owner).String() {
			ownerKey = key
		}
		keys = append(keys, key)
		kmems[key] = member
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the key owner
	idx := 0
	for idx < len(keys) {
		if keys[idx] == ownerKey {
			key := keys[idx]
			res = append(res, *kmems[key])
			break
		}
		idx++
	}

	// Find the closest(replica owners) members.
	for len(res) < count {
		idx++
		if idx >= len(keys) {
			idx = 0
		}
		key := keys[idx]
		res = append(res, *kmems[key])
	}
	return res, nil
}

func (s *state) copy() *state {
	ns := state{
		config:         s.config,
		hasher:         s.hasher,
		sortedSet:      make([]uint64, len(s.sortedSet)),
		partitionCount: s.partitionCount,
		loads:          make(map[string]float64),
		members:        make(map[string]*Member),
		partitions:     make(map[int]*Member),
		ring:           make(map[uint64]*Member),
	}

	// copy all values from the existing state
	copy(ns.sortedSet, s.sortedSet)

	for k, v := range s.loads {
		ns.loads[k] = v
	}

	for k, v := range s.members {
		ns.members[k] = v
	}

	for k, v := range s.partitions {
		ns.partitions[k] = v
	}

	for k, v := range s.ring {
		ns.ring[k] = v
	}

	return &ns
}
