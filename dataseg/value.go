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

func (v *ArrayValue) Size() int {
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

func (v *ArrayValue) Remove(index uint32) {
	v.metaSlab.Remove(index)
}

/*
func (v *ArrayValue) Elements() []Value {
	var v []Value
	serizable := v.metaSlab.Get(index)
	return serizable.GetValue()
}
*/
