package main

// from github.com/onflow/cadence/runtime/interpreter/value.go

type Value interface {
	// existing Value interface functions

	// New function to return Serializable
	GetSerizable() Serializable
}

// UInt32Value

type UInt32Value uint32

func (v UInt32Value) GetSerizable() Serializable {
	return &UInt32Serializable{v: v}
}

// ArrayValue

type ArrayValue struct {
	// metaSlab replaces values
	metaSlab *ArrayMetaSlab
}

func NewArrayValue(values []Value) *ArrayValue {
	metaSlab := &ArrayMetaSlab{
		id: generateStorageID(),
	}

	array := &ArrayValue{metaSlab: metaSlab}

	metaSlab.v = array

	for _, v := range values {
		metaSlab.Append(v.GetSerizable())
	}
	return array
}

func NewArrayValueFromEncodedData(data []byte) (*ArrayValue, error) {
	metaSlab := &ArrayMetaSlab{
		id: generateStorageID(),
	}

	array := &ArrayValue{metaSlab: metaSlab}

	metaSlab.v = array

	err := metaSlab.Decode(data)
	if err != nil {
		return nil, err
	}

	return array, nil
}

func (v *ArrayValue) GetSerizable() Serializable {
	return v.metaSlab
}

func (v *ArrayValue) Size() uint32 {
	return v.metaSlab.GetCount()
}

func (v *ArrayValue) Get(index uint32) (Value, error) {
	serizable, err := v.metaSlab.Get(index)
	if err != nil {
		return nil, err
	}
	return serizable.GetValue(), nil
}

func (v *ArrayValue) Append(value Value) {
	v.metaSlab.Append(value.GetSerizable())
}

func (v *ArrayValue) Remove(index uint32) error {
	return v.metaSlab.Remove(index)
}

func (v *ArrayValue) Insert(index uint32, value Value) error {
	return v.metaSlab.Insert(index, value.GetSerizable())
}

func (v *ArrayValue) Set(index uint32, value Value) error {
	return v.metaSlab.Set(index, value.GetSerizable())
}
