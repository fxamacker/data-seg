package main

type Segmentable interface {
	Split() (Segmentable, Segmentable, error)
	Merge(Segmentable) error
	ByteSize() uint32
}

type Slab interface {
	Storable
	Segmentable
}

type SlabStorage interface {
	Store(Slab)
	Retrieve(StorageID) (Slab, bool, error)
	Remove(StorageID)
}

// think of it as ledger
type BasicSlabStorage struct {
	slabs map[StorageID]Slab
}

func NewBasicSlabStorage() *BasicSlabStorage {
	return &BasicSlabStorage{slabs: make(map[StorageID]Slab)}
}

func (s *BasicSlabStorage) Retrieve(id StorageID) (Slab, bool, error) {
	slab, ok := s.slabs[id]
	return slab, ok, nil
}

func (s *BasicSlabStorage) Store(slab Slab) {
	s.slabs[slab.ID()] = slab
}

func (s *BasicSlabStorage) Remove(id StorageID) {
	delete(s.slabs, id)
}
