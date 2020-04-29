//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"

	"github.com/rhmap/store"
	"log"

	"github.com/blevesearch/bleve/search"
)

type storeSpillOverHeap struct {
	h             *store.Heap
	sortOrder     search.SortOrder
	path          string
	cachedScoring []bool
	cachedDesc    []bool
	dw            *docWrapper

	buf       []byte
	hitNumBuf []byte
}

// defaultChunkSize for the heap chunks.
var defaultChunkSize = int(10000)

// sortBytes is used for storing the sortOrder bytes.
type sortBytes struct {
	SortBytes [][]byte `json:"sortBytes"`
}

// docWrapper struct is used for helping with the
// json parsing since the original documentMatch struct
// has a few fields hidden from json parsing.
type docWrapper struct {
	Doc                *search.DocumentMatch      `json:documentMatch`
	InternalID         []byte                     `json:internalID`
	HitNumber          uint64                     `json:hitnumber`
	FieldTermLocations []search.FieldTermLocation `json:"fieldTermLocations,omitempty"`
}

func newStoreSpillOverHeap(capacity int, cachedScoring, cachedDesc []bool,
	sort search.SortOrder) *storeSpillOverHeap {
	dir, _ := ioutil.TempDir("", "topNHeap")
	rv := &storeSpillOverHeap{
		h: &store.Heap{
			LessFunc: func(a, b []byte) bool {
				asb := splitSortBytes(a)
				bsb := splitSortBytes(b)

				var aSort, bSort sortBytes
				err := json.Unmarshal(asb, &aSort)
				if err != nil {
					// TODO better handling here
					log.Printf("spilloverHeap: unmarshall err: %v, %q", err, asb)
				}
				err = json.Unmarshal(bsb, &bSort)
				if err != nil {
					log.Printf("spilloverHeap: unmarshall err: %v, %q", err, bsb)
				}

				c := 0
				for x := range sort {
					c = bytes.Compare(aSort.SortBytes[x], bSort.SortBytes[x])
					if c == 0 {
						continue
					}
					if cachedDesc[x] {
						c = -c
					}
					return -c < 0
				}
				// compare the hitNumber bytes
				c = bytes.Compare(aSort.SortBytes[len(sort)], bSort.SortBytes[len(sort)])
				return -c < 0
			},
			Heap: &store.Chunks{
				PathPrefix:     dir,
				FileSuffix:     ".heap",
				ChunkSizeBytes: defaultChunkSize,
			},
			Data: &store.Chunks{
				PathPrefix:     dir,
				FileSuffix:     ".data",
				ChunkSizeBytes: defaultChunkSize,
			},
		},
		sortOrder: sort,
	}

	rv.path = dir
	rv.cachedDesc = cachedDesc
	rv.cachedScoring = cachedScoring
	rv.buf = make([]byte, binary.MaxVarintLen16)
	rv.hitNumBuf = make([]byte, 8)
	return rv
}

func (c *storeSpillOverHeap) Close() error {
	return c.h.Close()
}

func splitDocBytes(in []byte) []byte {
	skip, n := binary.Uvarint(in)
	if n == 0 || int(skip) > len(in) {
		return nil
	}
	return in[binary.MaxVarintLen16+int(skip):]
}

func splitSortBytes(in []byte) []byte {
	skip, n := binary.Uvarint(in)
	if n == 0 || int(skip) > len(in) {
		return nil
	}
	return in[binary.MaxVarintLen16 : binary.MaxVarintLen16+int(skip)]
}

func (c *storeSpillOverHeap) Pop() (*search.DocumentMatch, error) {
	topBytes := heap.Pop(c.h).([]byte)
	docBytes := splitDocBytes(topBytes)
	var doc *search.DocumentMatch
	*c.dw = docWrapper{}
	err := json.Unmarshal(docBytes, &c.dw)
	if err != nil {
		// TODO better handling?
		log.Printf("pop: json unmarshal err: %v, docBytes: %q", err, docBytes)
		return nil, err
	}
	doc = c.dw.Doc
	doc.IndexInternalID = c.dw.InternalID
	doc.HitNumber = c.dw.HitNumber
	doc.FieldTermLocations = c.dw.FieldTermLocations
	return doc, nil
}

func (c *storeSpillOverHeap) docBytes(doc *search.DocumentMatch) []byte {
	c.dw = &docWrapper{Doc: doc,
		InternalID:         doc.IndexInternalID,
		HitNumber:          doc.HitNumber,
		FieldTermLocations: doc.FieldTermLocations,
	}

	docBytes, err := json.Marshal(c.dw)
	if err != nil {
		log.Printf("docBytes: json marshall, err: %v", err)
		return nil
	}

	return docBytes
}

func (c *storeSpillOverHeap) sortBytes(doc *search.DocumentMatch) []byte {
	rv := make([][]byte, len(doc.Sort)+1)
	for i, so := range c.sortOrder {
		if !c.cachedScoring[i] {
			rv[i] = []byte(doc.Sort[i])
		} else {
			rv[i] = so.ValueBytes(doc)
		}
	}

	binary.LittleEndian.PutUint64(c.hitNumBuf, doc.HitNumber)
	rv[len(doc.Sort)] = c.hitNumBuf

	tmp := sortBytes{SortBytes: rv}
	b, err := json.Marshal(&tmp)
	if err != nil {
		log.Printf("sortBytes: json marshall, err: %v", err)
	}
	return b
}

// The spilloverHeapStore works on byte slices. So for reducing the amount of
// data parsed during the heapify operations, each document is byte serialised
// into a format like => [sortOrderBytesLen|sortOrderBytes|docBytes].
// Thinking is that, during the heapify operations it only need to unmarshal upto
// the sortOrderBytes which decides the order.
func (c *storeSpillOverHeap) AddNotExceedingSize(doc *search.DocumentMatch,
	size int) *search.DocumentMatch {
	// get the sort bytes.
	sortBytes := c.sortBytes(doc)
	// encode the sort byte's length.
	binary.PutUvarint(c.buf, uint64(len(sortBytes)))
	sortBytes = append(c.buf, sortBytes...)
	// get the document bytes.
	dBytes := c.docBytes(doc)
	// form the byte slice to be stored.
	iv := append(sortBytes, dBytes...)

	heap.Push(c.h, iv)

	if c.h.Len() > size {
		doc, _ := c.Pop()
		return doc
	}
	return nil
}

func (c *storeSpillOverHeap) Final(skip int, fixup collectorFixup) (search.DocumentMatchCollection, error) {
	count := c.h.Len()
	size := count - skip
	if size <= 0 {
		return make(search.DocumentMatchCollection, 0), nil
	}

	rv := make(search.DocumentMatchCollection, size)
	for i := size - 1; i >= 0; i-- {
		doc, _ := c.Pop()
		rv[i] = doc
		err := fixup(doc)
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}
