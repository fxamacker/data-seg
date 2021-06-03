package main

import (
	"container/list"
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
	orderedHeaders list.List
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

func (a *ArraySlab) Insert(index uint32, v Serializable) error {
	if index >= uint32(len(a.elements)) {
		return fmt.Errorf("out of bounds")
	}

	// Update elements
	a.elements = append(a.elements, nil)
	copy(a.elements[index+1:], a.elements[index:])
	a.elements[index] = v

	// Update header
	a.header.size += v.ByteSize()
	a.header.count++
	return nil
}

func (a *ArraySlab) Set(index uint32, v Serializable) error {
	if index >= uint32(len(a.elements)) {
		return fmt.Errorf("out of bounds")
	}

	oldSize := a.elements[index].ByteSize()

	// Update elements
	a.elements[index] = v

	// Update header
	a.header.size = a.header.size - oldSize + v.ByteSize()

	return nil
}

func (a *ArraySlab) headerSize() uint32 {
	return 5
}

func (a *ArraySlab) Split() (*ArraySlab, error) {

	if len(a.elements) == 1 {
		// Can't split array with one element
		return nil, nil
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

	return newSlab, nil
}

func (a *ArraySlab) Merge(slab2 *ArraySlab) error {
	a.elements = append(a.elements, slab2.elements...)
	a.header.size += slab2.header.size
	a.header.count += slab2.header.count
	return nil
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
		var b []byte
		var err error
		b, err = e.Encode()
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

func (a *ArrayMetaSlab) IsConstantSized() bool { return false }

func (a *ArrayMetaSlab) ID() StorageID {
	return a.id
}

// TODO: count can be cached
func (a *ArrayMetaSlab) GetCount() uint32 {
	count := uint32(0)
	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		header := e.Value.(*ArraySlabHeader)
		count += header.count
	}
	return count
}

func (a *ArrayMetaSlab) Encode() ([]byte, error) {
	headerSize := 8 + a.orderedHeaders.Len()*8

	buf := make([]byte, headerSize)

	// Write metaslab id (4 bytes)
	binary.BigEndian.PutUint32(buf, uint32(a.id))

	// Write number of slabs (4 bytes)
	binary.BigEndian.PutUint32(buf[4:], uint32(a.orderedHeaders.Len()))

	// For each slab, write slab id (4 bytes) and slab size (4 bytes)
	i := 0
	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		header := e.Value.(*ArraySlabHeader)
		binary.BigEndian.PutUint32(buf[8+i*8:], uint32(header.id))
		binary.BigEndian.PutUint32(buf[8+i*8+4:], uint32(header.size))
		i++
	}

	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		header := e.Value.(*ArraySlabHeader)
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

		a.orderedHeaders.PushBack(slab.header)
	}

	return nil
}

func (a *ArrayMetaSlab) ByteSize() uint32 {
	var size uint32
	size = uint32(8) + uint32(a.orderedHeaders.Len())*8
	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		header := e.Value.(*ArraySlabHeader)
		size += header.size
	}
	return size
}

func (a *ArrayMetaSlab) Get(index uint32) (Serializable, error) {
	if a.orderedHeaders.Len() == 0 {
		return nil, fmt.Errorf("out of bounds")
	}

	startIndex := uint32(0)
	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		h := e.Value.(*ArraySlabHeader)
		if index >= startIndex && index < startIndex+uint32(h.count) {
			return h.slab.Get(index - startIndex)
		}
		startIndex += uint32(h.count)
	}

	return nil, fmt.Errorf("out of bounds")
}

func (a *ArrayMetaSlab) Append(v Serializable) error {
	lastHeader := a.orderedHeaders.Back()

	// Create new slab if
	// - there isn't any slab, or
	// - last slab size will exceed maxThreshold with new element

	if lastHeader == nil ||
		lastHeader.Value.(*ArraySlabHeader).size+v.ByteSize() > maxThreshold {

		header := &ArraySlabHeader{id: generateStorageID()}

		slab := &ArraySlab{header: header}

		header.slab = slab
		header.size = slab.headerSize()

		a.orderedHeaders.PushBack(slab.header)

		return slab.Append(v)
	}

	lastSlab := lastHeader.Value.(*ArraySlabHeader).slab
	return lastSlab.Append(v)
}

func (a *ArrayMetaSlab) Remove(index uint32) error {
	slabIndex := uint32(0)
	startIndex := uint32(0)
	var headerElement *list.Element

	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		h := e.Value.(*ArraySlabHeader)
		if index >= startIndex && index < startIndex+uint32(h.count) {
			headerElement = e
			slabIndex = index - startIndex
			break
		}
		startIndex += uint32(h.count)
	}

	if headerElement == nil {
		return fmt.Errorf("out of bounds")
	}

	slab := headerElement.Value.(*ArraySlabHeader).slab
	err := slab.Remove(slabIndex)
	if err != nil {
		return err
	}

	if slab.header.size < minThreshold {
		return a.merge(headerElement)
	}

	return nil
}

func (a *ArrayMetaSlab) Insert(index uint32, v Serializable) error {
	startIndex := uint32(0)
	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		h := e.Value.(*ArraySlabHeader)
		if index >= startIndex && index < startIndex+uint32(h.count) {
			err := h.slab.Insert(index-startIndex, v)
			if err != nil {
				return err
			}
			if h.size > uint32(maxThreshold) {
				a.split(e)
			}
			return nil
		}
		startIndex += uint32(h.count)
	}
	return nil
}

func (a *ArrayMetaSlab) Set(index uint32, v Serializable) error {
	startIndex := uint32(0)
	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		h := e.Value.(*ArraySlabHeader)
		if index >= startIndex && index < startIndex+uint32(h.count) {
			err := h.slab.Set(index-startIndex, v)
			if err != nil {
				return err
			}
			if h.size > uint32(maxThreshold) {
				a.split(e)
			} else if h.size < minThreshold {
				a.merge(e)
			}
			return nil
		}
		startIndex += uint32(h.count)
	}
	return nil
}

func (a *ArrayMetaSlab) merge(headerElement *list.Element) error {

	if a.orderedHeaders.Len() == 1 {
		return nil
	}

	header := headerElement.Value.(*ArraySlabHeader)
	slab := header.slab

	if headerElement.Prev() == nil {

		// First slab merges with next slab
		nextHeaderElement := headerElement.Next()
		nextSlab := nextHeaderElement.Value.(*ArraySlabHeader).slab

		// Merge with next slab
		slab.Merge(nextSlab)

		// Remove merged slab header
		a.orderedHeaders.Remove(nextHeaderElement)

		if header.size > maxThreshold {
			return a.split(headerElement)
		}

		return nil
	}

	if headerElement.Next() == nil {

		// Last slab merges with prev slab
		prevHeaderElement := headerElement.Prev()
		prevSlab := prevHeaderElement.Value.(*ArraySlabHeader).slab

		prevSlab.Merge(slab)

		// Remove merged (last) slab header
		a.orderedHeaders.Remove(headerElement)

		if prevSlab.header.size > maxThreshold {
			return a.split(prevHeaderElement)
		}

		return nil
	}

	prevHeaderElement := headerElement.Prev()
	prevHeader := prevHeaderElement.Value.(*ArraySlabHeader)

	nextHeaderElement := headerElement.Next()
	nextHeader := nextHeaderElement.Value.(*ArraySlabHeader)

	if prevHeader.size <= nextHeader.size {
		// Merge with previous slab
		prevHeader.slab.Merge(slab)

		// remove merged slab header
		a.orderedHeaders.Remove(headerElement)

		if prevHeader.size > maxThreshold {
			return a.split(prevHeaderElement)
		}
		return nil

	} else {

		// Merge with next slab
		slab.Merge(nextHeader.slab)

		// Remove merged slab header
		a.orderedHeaders.Remove(nextHeaderElement)

		if header.size > maxThreshold {
			return a.split(headerElement)
		}
		return nil
	}
}

func (a *ArrayMetaSlab) split(headerElement *list.Element) error {
	header := headerElement.Value.(*ArraySlabHeader)

	newSlab, err := header.slab.Split()
	if err != nil {
		return err
	}
	if newSlab == nil {
		return nil
	}

	a.orderedHeaders.InsertAfter(newSlab.header, headerElement)
	return nil
}

// Print is intended for debugging purpose only
func (a *ArrayMetaSlab) Print() {
	fmt.Println("============= array slabs ================")
	i := 0
	for e := a.orderedHeaders.Front(); e != nil; e = e.Next() {
		h := e.Value.(*ArraySlabHeader)
		fmt.Printf("slab %d, id %d, count %d, size %d\n", i, h.id, h.count, h.size)
		fmt.Printf("[")
		for _, e := range h.slab.elements {
			fmt.Printf("%[1]v (%[1]T), ", e.GetValue())
		}
		fmt.Printf("]\n")
		i++
	}
	fmt.Println("==========================================")
}
