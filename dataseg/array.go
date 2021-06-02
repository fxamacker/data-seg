package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type ArraySlabHeader struct {
	id    StorageID
	slab  *ArraySlab // remove this when switching to SlabStorage
	count uint32     // number of elements in ArraySlab
	size  uint32     // sum of all element size + array header size
}

// ArraySlab implements Slab interface
type ArraySlab struct {
	header   *ArraySlabHeader
	elements []Serializable
}

type ArrayMetaSlab struct {
	id             StorageID
	orderedHeaders []*ArraySlabHeader // TODO: orderedHeaders can be a linked list
	v              *ArrayValue
}

func (a *ArraySlab) Get(index uint32) (Serializable, error) {
	if int(index) >= len(a.elements) {
		return nil, fmt.Errorf("out of bounds")
	}
	return a.elements[index], nil
}

func (a *ArraySlab) Append(v Serializable) error {
	a.elements = append(a.elements, v)
	a.header.size += v.ByteSize()
	a.header.count++
	return nil
}

func (a *ArraySlab) Remove(index uint32) error {
	if int(index) >= len(a.elements) {
		return fmt.Errorf("out of bounds")
	}

	// Update header
	oldSize := a.elements[index].ByteSize()
	a.header.size -= oldSize
	a.header.count--

	// Update elements
	copy(a.elements[index:], a.elements[index+1:])
	a.elements = a.elements[:len(a.elements)-1]

	return nil
}

func (a *ArraySlab) headerSize() uint32 {
	return 5
}

func (a *ArraySlab) Split() *ArraySlab {

	if len(a.elements) == 1 {
		// Can't split array with one element
		return nil
	}

	// this compute the ceil of split keep the first part with more members (optimized for append operations)
	size := a.header.size
	d := float64(size) / float64(2)
	breakPoint := int(math.Ceil(d))

	newSlabStartIndex := 0
	slab1Size := a.headerSize()
	for i, v := range a.elements {
		slab1Size += v.ByteSize()
		if slab1Size > uint32(breakPoint) {
			newSlabStartIndex = i + 1
			break
		}
	}

	if newSlabStartIndex == len(a.elements) {
		// Split last element from the rest of elements
		newSlabStartIndex = len(a.elements) - 1
		slab1Size = a.header.size - a.elements[len(a.elements)-1].ByteSize()
	}

	newSlabHeader := &ArraySlabHeader{
		id:    generateStorageID(),
		count: a.header.count - uint32(newSlabStartIndex),
		size:  a.header.size - slab1Size + a.headerSize(),
	}
	newSlab := &ArraySlab{
		header:   newSlabHeader,
		elements: a.elements[newSlabStartIndex:],
	}
	newSlabHeader.slab = newSlab

	a.elements = a.elements[:newSlabStartIndex]
	a.header.size = slab1Size
	a.header.count = uint32(newSlabStartIndex)

	return newSlab
}

func (a *ArraySlab) Merge(slab2 *ArraySlab) {
	a.elements = append(a.elements, slab2.elements...)
	a.header.size += slab2.header.size
	a.header.count += slab2.header.count
}

func (a *ArraySlab) ID() StorageID {
	return a.header.id
}

func (a *ArraySlab) Encode() ([]byte, error) {
	buf := make([]byte, a.ByteSize())

	// Array head
	buf[0] = 0x80 | byte(26)
	binary.BigEndian.PutUint32(buf[1:], uint32(len(a.elements)))

	index := 5
	for _, e := range a.elements {
		b, err := e.Encode()
		if err != nil {
			return nil, err
		}
		n := copy(buf[index:], b)
		index += n
	}

	return buf, nil
}

func (a *ArraySlab) Decode(data []byte) error {

	if len(data) < int(a.headerSize()) {
		return errors.New("wrong byte size for array slab")
	}
	if data[0] != 0x80|byte(26) {
		return errors.New("wrong data for array slab")
	}

	count := binary.BigEndian.Uint32(data[1:])

	data = data[5:]
	a.elements = make([]Serializable, count)
	for i := 0; i < int(count); i++ {
		var s Serializable
		var err error
		s, data, err = decodeSerializable(data)
		if err != nil {
			return err
		}
		a.elements[i] = s
	}
	return nil
}

func (a *ArraySlab) ByteSize() uint32 {
	return a.header.size // Array head size + element size cached in slab header
}

func (a *ArrayMetaSlab) GetValue() Value {
	return a.v
}

func (a *ArrayMetaSlab) ID() StorageID {
	return a.id
}

func (a *ArrayMetaSlab) GetCount() uint32 {
	count := uint32(0)
	for _, header := range a.orderedHeaders {
		count += header.count
	}
	return count
}

func (a *ArrayMetaSlab) Encode() ([]byte, error) {
	headerSize := 8 + len(a.orderedHeaders)*8

	buf := make([]byte, headerSize)

	// Write metaslab id (4 bytes)
	binary.BigEndian.PutUint32(buf, uint32(a.id))

	// Write number of slabs (4 bytes)
	binary.BigEndian.PutUint32(buf[4:], uint32(len(a.orderedHeaders)))

	// For each slab, write slab id (4 bytes) and slab size (4 bytes)
	for i, header := range a.orderedHeaders {
		binary.BigEndian.PutUint32(buf[8+i*8:], uint32(header.id))
		binary.BigEndian.PutUint32(buf[8+i*8+4:], uint32(header.size))
	}

	for _, header := range a.orderedHeaders {
		b, err := header.slab.Encode()
		if err != nil {
			return nil, err
		}
		buf = append(buf, b...)
	}

	return buf, nil
}

func (a *ArrayMetaSlab) Decode(data []byte) error {
	if len(data) < 8 {
		return errors.New("too short for array meta slab")
	}

	a.id = StorageID(binary.BigEndian.Uint32(data[:4]))

	slabCount := binary.BigEndian.Uint32(data[4:])
	if slabCount == 0 {
		return nil
	}

	slabData := make([][2]uint32, slabCount)

	index := 8
	for i := 0; i < int(slabCount); i++ {
		id := binary.BigEndian.Uint32(data[index:])
		size := binary.BigEndian.Uint32(data[index+4:])
		index += 8
		slabData[i] = [2]uint32{id, size}
	}

	for _, sd := range slabData {
		slab := &ArraySlab{
			header: &ArraySlabHeader{
				id: StorageID(sd[0]),
			},
		}

		err := slab.Decode(data[index : index+int(sd[1])])
		if err != nil {
			return err
		}

		index = index + int(sd[1])

		slab.header.slab = slab
		slab.header.size = sd[1]
		slab.header.count = uint32(len(slab.elements))

		a.orderedHeaders = append(a.orderedHeaders, slab.header)
	}

	return nil
}

func (a *ArrayMetaSlab) ByteSize() uint32 {
	var size uint32
	size = uint32(8) + uint32(len(a.orderedHeaders))*8
	for _, header := range a.orderedHeaders {
		size += header.size
	}
	return size
}

func (a *ArrayMetaSlab) Get(index uint32) (Serializable, error) {
	if len(a.orderedHeaders) == 0 {
		return nil, fmt.Errorf("out of bounds")
	}

	startIndex := uint32(0)
	for _, h := range a.orderedHeaders {
		if index >= startIndex && index < startIndex+uint32(h.count) {
			return h.slab.Get(index - startIndex)
		}
		startIndex += uint32(h.count)
	}

	return nil, fmt.Errorf("out of bounds")
}

func (a *ArrayMetaSlab) Append(v Serializable) error {
	// Create new slab if
	// - there isn't any slab, or
	// - last slab size will exceed maxThreshold with new element
	if len(a.orderedHeaders) == 0 ||
		a.orderedHeaders[len(a.orderedHeaders)-1].size+v.ByteSize() > maxThreshold {

		header := &ArraySlabHeader{id: generateStorageID()}

		slab := &ArraySlab{header: header}

		header.slab = slab
		header.size = slab.headerSize()

		a.orderedHeaders = append(a.orderedHeaders, slab.header)

		return slab.Append(v)
	}

	lastSlab := a.orderedHeaders[len(a.orderedHeaders)-1].slab
	return lastSlab.Append(v)
}

func (a *ArrayMetaSlab) Remove(index uint32) error {
	foundHeadIndex := -1
	slabIndex := uint32(0)
	startIndex := uint32(0)
	for i, h := range a.orderedHeaders {
		if index >= startIndex && index < startIndex+uint32(h.count) {
			foundHeadIndex = i
			slabIndex = index - startIndex
			break
		}
		startIndex += uint32(h.count)
	}

	if foundHeadIndex < 0 {
		return fmt.Errorf("out of bounds")
	}

	slab := a.orderedHeaders[foundHeadIndex].slab
	err := slab.Remove(slabIndex)
	if err != nil {
		return err
	}

	if slab.header.size < minThreshold {
		return a.merge(foundHeadIndex)
	}

	return nil
}

func (a *ArrayMetaSlab) merge(headerIndex int) error {
	if len(a.orderedHeaders) == 1 {
		return nil
	}

	if headerIndex == 0 {

		// first slab merges with next slab
		nextSlab := a.orderedHeaders[headerIndex+1].slab
		a.orderedHeaders[headerIndex].slab.Merge(nextSlab)

		// remove merged slab header
		copy(a.orderedHeaders[headerIndex+1:], a.orderedHeaders[headerIndex+2:])
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

		if a.orderedHeaders[headerIndex].size > maxThreshold {
			return a.split(headerIndex)
		}

		return nil
	}

	if headerIndex == len(a.orderedHeaders)-1 {

		// last slab merges with prev slab
		prevSlab := a.orderedHeaders[headerIndex-1].slab
		prevSlab.Merge(a.orderedHeaders[headerIndex].slab)

		// remove merged (last) slab header
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

		if a.orderedHeaders[len(a.orderedHeaders)-1].size > maxThreshold {
			return a.split(len(a.orderedHeaders) - 1)
		}

		return nil
	}

	prevHeader := a.orderedHeaders[headerIndex-1]
	nextHeader := a.orderedHeaders[headerIndex+1]

	if prevHeader.size <= nextHeader.size {
		// Merge with previous slab
		prevHeader.slab.Merge(a.orderedHeaders[headerIndex].slab)

		// remove merged slab header
		copy(a.orderedHeaders[headerIndex:], a.orderedHeaders[headerIndex+1:])
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

		if prevHeader.size > maxThreshold {
			return a.split(headerIndex - 1)
		}
		return nil

	} else {

		// Merge with next slab
		a.orderedHeaders[headerIndex].slab.Merge(nextHeader.slab)

		// remove merged slab header
		copy(a.orderedHeaders[headerIndex+1:], a.orderedHeaders[headerIndex+2:])
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

		if a.orderedHeaders[headerIndex].size > maxThreshold {
			return a.split(headerIndex)
		}
		return nil
	}
}

func (a *ArrayMetaSlab) split(headerIndex int) error {
	slab := a.orderedHeaders[headerIndex].slab
	newSlab := slab.Split()
	if newSlab == nil {
		return nil
	}

	a.orderedHeaders = append(a.orderedHeaders, nil)
	copy(a.orderedHeaders[headerIndex+2:], a.orderedHeaders[headerIndex+1:])
	a.orderedHeaders[headerIndex+1] = newSlab.header
	return nil
}

// Print is intended for debugging purpose only
func (a *ArrayMetaSlab) Print() {
	fmt.Println("============= array slabs ================")
	for i, h := range a.orderedHeaders {
		fmt.Printf("slab %d, id %d, count %d, size %d\n", i, h.id, h.count, h.size)
		fmt.Printf("[")
		for _, e := range h.slab.elements {
			fmt.Printf("%[1]v (%[1]T), ", e.GetValue())
		}
		fmt.Printf("]\n")
	}
	fmt.Println("==========================================")
}
