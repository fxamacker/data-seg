package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type StorageID uint32

type Serializable interface {
	Encode() ([]byte, error)
	Decode([]byte) error
	ByteSize() uint32

	GetValue() Value
}

type Storable interface {
	Serializable
	ID() StorageID
}

type Storage interface {
	Store(Storable) error
	Retrieve(StorageID) (Storable, bool, error)
	Remove(StorageID)
}

const (
	cborTagUInt32Value = 163
)

type UInt32Serializable struct {
	v      UInt32Value
	cached []byte
}

// Encode encodes UInt32Value as
// cbor.Tag{
//		Number:  cborTagUInt32Value,
//		Content: Uint32(v),
// }
func (s *UInt32Serializable) Encode() ([]byte, error) {
	// Reuse cached data
	if len(s.cached) > 0 {
		return s.cached, nil
	}

	buf := make([]byte, s.ByteSize())

	buf[0] = 0xd8
	buf[1] = cborTagUInt32Value
	buf[2] = 0 | byte(26)
	binary.BigEndian.PutUint32(buf[3:], uint32(s.v))

	return buf, nil
}

func (s *UInt32Serializable) Decode(b []byte) error {
	if uint32(len(b)) < s.ByteSize() {
		return errors.New("too short for Int32Value type")
	}

	if !bytes.Equal([]byte{0xd8, cborTagUInt32Value, 26}, b[:3]) {
		return errors.New("not Int32Value type")
	}

	s.v = UInt32Value(binary.BigEndian.Uint32(b[3:]))
	s.cached = b
	return nil
}

// ByteSize() returns consistent size at the expense of compact data.
func (s *UInt32Serializable) ByteSize() uint32 {
	// tag number (2 bytes) + content content (5 bytes)
	return 7
}

func (s *UInt32Serializable) GetValue() Value {
	return s.v
}

func decodeSerializable(data []byte) (Serializable, []byte, error) {
	if len(data) < 2 {
		return nil, data, errors.New("wrong data size")
	}

	if bytes.Equal([]byte{0xd8, cborTagUInt32Value}, data[:2]) {
		s := &UInt32Serializable{}
		err := s.Decode(data)
		if err != nil {
			return nil, data, err
		}
		return s, data[s.ByteSize():], nil
	}

	return nil, nil, errors.New("not supported serializable format")
}
