package main

import "fmt"

const targetThreshold = 60

const minThreshold = targetThreshold / 4   // 15
const maxThreshold = targetThreshold * 1.5 // 90
//const maxItemSize = 6

func newArrayExample() {

	// Create ArrayValue with Values
	values := make([]Value, 20)
	for i := 0; i < len(values); i++ {
		values[i] = UInt32Value(i)
	}

	fmt.Printf("Create ArrayValue with cadence values %v\n", values)
	array := NewArrayValue(values)

	// Print underlying slab layout
	array.metaSlab.Print()

	verifyArrayElements(array, values)

	data, err := array.GetSerizable().Encode()
	if err != nil {
		fmt.Printf("Encode error %v\n", err)
		return
	}

	//fmt.Printf("encoded data 0x%x\n", data)

	fmt.Printf("Recreate ArrayValue with encoded data\n")

	// Reconstruct array using encoded data
	array2, err := NewArrayValueFromEncodedData(data)
	if err != nil {
		fmt.Printf("NewArrayValueFromEncodedData(0x%x) error %v\n", data, err)
		return
	}

	array2.metaSlab.Print()

	verifyArrayElements(array2, values)
}

func removeArrayExample() {

	// Create ArrayValue with Values
	values := make([]Value, 20)
	for i := 0; i < len(values); i++ {
		values[i] = UInt32Value(i)
	}

	fmt.Printf("Create ArrayValue with cadence values %v\n", values)
	array := NewArrayValue(values)

	// Print underlying slab layout
	array.metaSlab.Print()

	verifyArrayElements(array, values)

	// Remove last 7 elements
	array.Remove(uint32(array.Size() - 1))
	array.Remove(uint32(array.Size() - 1))
	array.Remove(uint32(array.Size() - 1))
	array.Remove(uint32(array.Size() - 1))
	array.Remove(uint32(array.Size() - 1))
	array.Remove(uint32(array.Size() - 1))
	array.Remove(uint32(array.Size() - 1))

	fmt.Printf("Remove last 7 elements which triggers a merge and then a split of merged slab\n")
	array.metaSlab.Print()
	/*
		data, err := array.GetSerizable().Encode()
		if err != nil {
			fmt.Printf("Encode error %v\n", err)
			return
		}

		//fmt.Printf("encoded data 0x%x\n", data)

		fmt.Printf("Recreate ArrayValue with encoded data\n")

		// Reconstruct array using encoded data
		array2, err := NewArrayValueFromEncodedData(data)
		if err != nil {
			fmt.Printf("NewArrayValueFromEncodedData(0x%x) error %v\n", data, err)
			return
		}

		array2.metaSlab.Print()

	*/
	verifyArrayElements(array, values[:len(values)-7])
}

func verifyArrayElements(array *ArrayValue, values []Value) {
	if array.Size() != uint32(len(values)) {
		fmt.Printf("wrong number of elements, got %d, want %d", array.Size(), len(values))
		return
	}

	// Get and verify each element
	for i := uint32(0); i < array.Size(); i++ {
		v, err := array.Get(uint32(i))
		if err != nil {
			fmt.Printf("array.Get(%d) error %v\n", i, err)
		}
		if v != values[i] {
			fmt.Printf("array.Get(%d) returned %[1]v (%[1]T), want %[2]v (%[2]T)\n", i, v, values[i])
		}
	}
}

func main() {
	newArrayExample()

	fmt.Println()
	fmt.Println()

	removeArrayExample()
}

// TODO add equal functionaity to create a list of values and compare it to an array
// so we can have test with randomize updates

// TODO add benchmarking on delays
// add proper testing to each componenet
