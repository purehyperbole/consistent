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

package consistent

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"sync/atomic"
	"testing"
)

func newConfig() Config {
	return Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
}

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

type hasher struct{}

func (hs hasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func TestConsistentAdd(t *testing.T) {
	cfg := newConfig()
	c := New[*testMember](nil, cfg)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(&member)
	}
	for member := range members {
		found := false
		for _, mem := range c.GetMembers() {
			if member == mem.String() {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s could not be found", member)
		}
	}
}

func TestConsistentRemove(t *testing.T) {
	members := []*testMember{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, &member)
	}
	cfg := newConfig()
	c := New[*testMember](members, cfg)
	if len(c.GetMembers()) != len(members) {
		t.Fatalf("inserted member count is different")
	}
	for _, member := range members {
		c.Remove(member.String())
	}
	if len(c.GetMembers()) != 0 {
		t.Fatalf("member count should be zero")
	}
}

func TestConsistentLoad(t *testing.T) {
	members := []*testMember{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, &member)
	}
	cfg := newConfig()
	c := New[*testMember](members, cfg)
	if len(c.GetMembers()) != len(members) {
		t.Fatalf("inserted member count is different")
	}
	maxLoad := c.AverageLoad()
	for member, load := range c.LoadDistribution() {
		if load > maxLoad {
			t.Fatalf("%s exceeds max load. Its load: %f, max load: %f", member, load, maxLoad)
		}
	}
}

func TestConsistentLocateKey(t *testing.T) {
	cfg := newConfig()
	c := New[*testMember](nil, cfg)
	key := []byte("Olric")
	res := c.LocateKey(key)
	if res != nil {
		t.Fatalf("This should be nil: %v", res)
	}
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(&member)
	}
	res = c.LocateKey(key)
	if res == nil {
		t.Fatalf("This shouldn't be nil: %v", res)
	}
}

func TestConsistentInsufficientMemberCount(t *testing.T) {
	members := []*testMember{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, &member)
	}
	cfg := newConfig()
	c := New[*testMember](members, cfg)
	key := []byte("Olric")
	_, err := c.GetClosestN(key, 30)
	if err != ErrInsufficientMemberCount {
		t.Fatalf("Expected ErrInsufficientMemberCount(%v), Got: %v", ErrInsufficientMemberCount, err)
	}
}

func TestConsistentClosestMembers(t *testing.T) {
	members := []*testMember{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, &member)
	}
	cfg := newConfig()
	c := New[*testMember](members, cfg)
	key := []byte("Olric")
	closestn, err := c.GetClosestN(key, 2)
	if err != nil {
		t.Fatalf("Expected nil, Got: %v", err)
	}
	if len(closestn) != 2 {
		t.Fatalf("Expected closest member count is 2. Got: %d", len(closestn))
	}
	partID := c.FindPartitionID(key)
	owner := c.GetPartitionOwner(partID)
	for i, cl := range closestn {
		if i != 0 && cl.String() == (*owner).String() {
			t.Fatalf("Backup is equal the partition owner: %s", (*owner).String())
		}
	}
}

func BenchmarkAddRemove(b *testing.B) {
	cfg := newConfig()
	c := New[*testMember](nil, cfg)
	b.ResetTimer()

	var p int64

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			member := testMember(fmt.Sprintf("node-%d-%d", atomic.AddInt64(&p, 1), i))
			c.Add(&member)
			c.Remove(member.String())
			i++
		}
	})
}

func BenchmarkLocateKey(b *testing.B) {
	cfg := newConfig()
	c := New[*testMember](nil, cfg)
	m1 := testMember("node1")
	m2 := testMember("node2")

	c.Add(&m1)
	c.Add(&m2)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := []byte("key" + strconv.Itoa(i))
			c.LocateKey(key)
			i++
		}
	})
}

func BenchmarkGetClosestN(b *testing.B) {
	cfg := newConfig()
	c := New[*testMember](nil, cfg)
	for i := 0; i < 10; i++ {
		m := testMember(fmt.Sprintf("node%d", i))
		c.Add(&m)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := []byte("key" + strconv.Itoa(i))
			c.GetClosestN(key, 3)
			i++
		}
	})
}
