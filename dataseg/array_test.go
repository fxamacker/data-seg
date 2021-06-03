package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewArray(t *testing.T) {
	// TODO:
	// - create non-empty array , serialize it, deserialize it, verify array content

	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		array := NewArrayValue(nil)

		b, err := array.GetSerizable().Encode()
		require.NoError(t, err)
		assert.Equal(t, len(b), 8)                 // meta slab id (4 bytes) + slab count (4 bytes)
		assert.Equal(t, []byte{0, 0, 0, 0}, b[4:]) // slab count is 0

		array2, err := NewArrayValueFromEncodedData(b)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), array2.Size())
	})

	t.Run("non-empty-one-slab", func(t *testing.T) {
		values := make([]Value, 2)
		for i := 0; i < len(values); i++ {
			values[i] = UInt32Value(i)
		}

		array := NewArrayValue(values)

		b, err := array.GetSerizable().Encode()
		require.NoError(t, err)
		assert.Equal(t, len(b), 8+8+19)                // meta slab id (4 bytes) + slab count (4 bytes) + slab 1 id (4 bytes) + slab 1 size (4 bytes) + slab 1 (19 bytes)
		assert.Equal(t, []byte{0, 0, 0, 1}, b[4:8])    // slab count
		assert.Equal(t, []byte{0, 0, 0, 19}, b[12:16]) // slab 1 size

		array2, err := NewArrayValueFromEncodedData(b)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(values)), array2.Size())

		for i := 0; i < len(values); i++ {
			v, err := array2.Get(uint32(i))
			require.NoError(t, err)
			assert.Equal(t, UInt32Value(i), v)
		}
	})

	t.Run("non-empty-multi-slabs", func(t *testing.T) {
		values := make([]Value, 20)
		for i := 0; i < len(values); i++ {
			values[i] = UInt32Value(i)
		}

		array := NewArrayValue(values)

		b, err := array.GetSerizable().Encode()
		require.NoError(t, err)

		array2, err := NewArrayValueFromEncodedData(b)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(values)), array2.Size())

		for i := 0; i < len(values); i++ {
			v, err := array2.Get(uint32(i))
			require.NoError(t, err)
			assert.Equal(t, UInt32Value(i), v)
		}
	})
}

func TestArrayAppend(t *testing.T) {

	values := make([]Value, 20)
	for i := 0; i < len(values); i++ {
		values[i] = UInt32Value(i)
	}

	array := NewArrayValue(nil)

	const arraySize = uint32(20)
	for i := 0; i < len(values); i++ {
		array.Append(UInt32Value(i))
	}

	b, err := array.GetSerizable().Encode()
	require.NoError(t, err)

	array2, err := NewArrayValueFromEncodedData(b)
	require.NoError(t, err)
	assert.Equal(t, arraySize, array2.Size())

	for i := 0; i < len(values); i++ {
		v, err := array2.Get(uint32(i))
		require.NoError(t, err)
		assert.Equal(t, UInt32Value(i), v)
	}
}

func TestArrayRemove(t *testing.T) {
	t.Run("fail", func(t *testing.T) {
		array := NewArrayValue(nil)
		err := array.Remove(0)
		require.Error(t, err)
	})

	t.Run("no slab merge/split", func(t *testing.T) {
		values := make([]Value, 2)
		for i := 0; i < len(values); i++ {
			values[i] = UInt32Value(i)
		}

		array := NewArrayValue(values)
		size := array.Size()
		for i := 0; i < len(values); i++ {
			err := array.Remove(0)
			require.NoError(t, err)

			size--
			assert.Equal(t, size, array.Size())
		}

		b, err := array.GetSerizable().Encode()
		require.NoError(t, err)

		array2, err := NewArrayValueFromEncodedData(b)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), array2.Size())
	})

	t.Run("slab merge", func(t *testing.T) {
		values := make([]Value, 15)
		for i := 0; i < len(values); i++ {
			values[i] = UInt32Value(i)
		}

		array := NewArrayValue(values)
		assert.True(t, array.metaSlab.orderedHeaders.Len() == 2)

		var err error

		err = array.Remove(0)
		require.NoError(t, err)

		err = array.Remove(0)
		require.NoError(t, err)

		err = array.Remove(uint32(array.Size() - 1))
		require.NoError(t, err)

		err = array.Remove(uint32(array.Size() - 1))
		require.NoError(t, err)

		assert.True(t, array.metaSlab.orderedHeaders.Len() == 1)
	})
}

func TestArrayInsert(t *testing.T) {

	array := NewArrayValue([]Value{UInt32Value(2)})

	err := array.Insert(0, UInt32Value(0))
	require.NoError(t, err)

	array.Insert(1, UInt32Value(1))
	require.NoError(t, err)

	assert.Equal(t, uint32(3), array.Size())

	b, err := array.GetSerizable().Encode()
	require.NoError(t, err)
	assert.True(t, len(b) > 8)

	array2, err := NewArrayValueFromEncodedData(b)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), array2.Size())

	for i := uint32(0); i < array2.Size(); i++ {
		v, err := array2.Get(uint32(i))
		require.NoError(t, err)
		assert.Equal(t, UInt32Value(i), v)
	}
}

func TestArraySet(t *testing.T) {
	array := NewArrayValue([]Value{UInt32Value(0), UInt32Value(1), UInt32Value(2)})

	array.Set(0, UInt32Value(3))
	array.Set(1, UInt32Value(4))
	array.Set(2, UInt32Value(5))

	assert.Equal(t, uint32(3), array.Size())

	b, err := array.GetSerizable().Encode()
	require.NoError(t, err)
	assert.True(t, len(b) > 8)

	array2, err := NewArrayValueFromEncodedData(b)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), array2.Size())

	for i := uint32(0); i < array2.Size(); i++ {
		v, err := array2.Get(uint32(i))
		require.NoError(t, err)
		assert.Equal(t, UInt32Value(i+3), v)
	}
}
