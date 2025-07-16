package bloom

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	n1 := []byte("Bess")
	n2 := []byte("Jane")
	n3 := []byte("Emma")

	f.Add(n1)
	n3a := f.TestAndAdd(n3)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	n3b := f.Test(n3)

	assert.True(t, n1b, "%v should be in", n1)
	assert.False(t, n2b, "%v should not be in", n2)
	assert.False(t, n3a, "%v should not be in the first time we look", n3)
	assert.True(t, n3b, "%v should be in the second time we look", n3)
}

func TestNewWithLowNumbers(t *testing.T) {
	f := NewBloomFilter(0, 0)
	assert.Equal(t, uint(1), f.hashNum, "hashNum should be 1")
	assert.Equal(t, uint(1), f.arraySize, "arraySize should be 1")
}

func TestCopy(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	n1 := []byte("f")
	f.Add(n1)

	g := f.Copy()
	n2 := []byte("g")
	g.Add(n2)

	assert.True(t, f.Test(n1), "original should contain n1")
	assert.True(t, g.Test(n1), "copy should contain n1")
	assert.False(t, f.Test(n2), "original should not contain n2")
	assert.True(t, g.Test(n2), "copy should contain n2")
}

func TestFrom(t *testing.T) {
	k := uint(5)
	data := make([]uint64, 10)
	test := []byte("test")

	bf := From(data, k)
	assert.Equal(t, k, bf.numberOfHashFunctions(), "constant hashNum mismatch")
	assert.Equal(t, uint(len(data)*64), bf.Cap(), "capacity mismatch")
	assert.False(t, bf.Test(test), "should not contain value before Add")

	bf.Add(test)
	assert.True(t, bf.Test(test), "should contain value after Add")

	// confirm persistence via underlying data
	bf2 := From(data, k)
	assert.True(t, bf2.Test(test), "new filter should contain value from original data")
}

func TestBasicUint32(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	n1 := make([]byte, 4)
	n2 := make([]byte, 4)
	n3 := make([]byte, 4)
	n4 := make([]byte, 4)
	n5 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, 100)
	binary.BigEndian.PutUint32(n2, 101)
	binary.BigEndian.PutUint32(n3, 102)
	binary.BigEndian.PutUint32(n4, 103)
	binary.BigEndian.PutUint32(n5, 104)
	f.Add(n1)
	n3a := f.TestAndAdd(n3)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	n3b := f.Test(n3)
	n5a := f.TestOrAdd(n5)
	n5b := f.Test(n5)
	f.Test(n4)
	assert.True(t, n1b, "%v should be in", n1)
	assert.False(t, n2b, "%v should not be in", n3)
	assert.False(t, n3a, "%v should not be in the first time we look", n3)
	assert.True(t, n3b, "%v should be in the second time we look")
	assert.False(t, n5a, "%v should not be in the first time we look", n5)
	assert.True(t, n5b, "%v should be in the first time we look")
}

func TestString(t *testing.T) {
	f := NewWithEstimates(1000, 0.001)
	n1 := "Love"
	n2 := "is"
	n3 := "in"
	n4 := "bloom"
	n5 := "blooms"
	f.AddString(n1)
	n3a := f.TestAndAddString(n3)
	n1b := f.TestString(n1)
	n2b := f.TestString(n2)
	n3b := f.TestString(n3)
	n5a := f.TestOrAddString(n5)
	n5b := f.TestString(n5)
	f.TestString(n4)
	assert.True(t, n1b, "%v should be in", n1)
	assert.False(t, n2b, "%v should not be in the first time we look", n3)
	assert.False(t, n3a, "%v should not be in the first time we look", n3)
	assert.True(t, n3b, "%v should be in the second")
	assert.False(t, n5a, "%v should not be in the first time we look", n5)
	assert.True(t, n5b, "%v should be in the first time we look")
}

func testEstimated(n uint, maxFp float64, t *testing.T) {
	m, k := EstimateParameters(n, maxFp)
	fpRate := EstimateFalsePositiveRate(m, k, n)
	if fpRate > 1.5*maxFp {
		t.Errorf("False positive rate too high: n: %v; arraySize: %v; hashNum: %v; maxFp: %f; fpRate: %f, fpRate/maxFp: %f", n, m, k, maxFp, fpRate, fpRate/maxFp)
	}
}

func TestEstimated1000_0001(t *testing.T) { testEstimated(1000, 0.000100, t) }

func TestEstimated10000_0001(t *testing.T) { testEstimated(10000, 0.000100, t) }

func TestEstimated100000_0001(t *testing.T) { testEstimated(100000, 0.000100, t) }

func TestEstimated1000_001(t *testing.T) { testEstimated(1000, 0.001000, t) }

func TestEstimated10000_001(t *testing.T) { testEstimated(10000, 0.001000, t) }

func TestEstimated100000_001(t *testing.T) { testEstimated(100000, 0.001000, t) }

func TestEstimated1000_01(t *testing.T) { testEstimated(1000, 0.010000, t) }

func TestEstimated10000_01(t *testing.T) { testEstimated(10000, 0.010000, t) }

func TestEstimated100000_01(t *testing.T) { testEstimated(100000, 0.010000, t) }

// The following function courtesy of Nick @turgon
// This helper function ranges over the input data, applying the hashing
// which returns the bit locations to set in the filter.
// For each location, increment a counter for that bit address.
//
// If the Bloom Filter's location() method distributes locations uniformly
// at random, a property it should inherit from its hash function, then
// each bit location in the filter should end up with roughly the same
// number of hits.  Importantly, the value of hashNum should not matter.
//
// Once the results are collected, we can run a chi squared goodness of fit
// test, comparing the result histogram with the uniform distribition.
// This yields a test statistic with degrees-of-freedom of arraySize-1.
func chiTestBloom(m, k, rounds uint, elements [][]byte) (succeeds bool) {
	f := NewBloomFilter(m, k)
	results := make([]uint, m)
	chi := make([]float64, m)

	for _, data := range elements {
		h := baseHashes(data)
		for i := uint(0); i < f.hashNum; i++ {
			results[f.location(h, i)]++
		}
	}

	// Each element of results should contain the same value: hashNum * rounds / arraySize.
	// Let's run a chi-square goodness of fit and see how it fares.
	var chiStatistic float64
	e := float64(k*rounds) / float64(m)
	for i := uint(0); i < m; i++ {
		chi[i] = math.Pow(float64(results[i])-e, 2.0) / e
		chiStatistic += chi[i]
	}

	// this tests at significant level 0.005 up to 20 degrees of freedom
	table := [20]float64{
		7.879, 10.597, 12.838, 14.86, 16.75, 18.548, 20.278,
		21.955, 23.589, 25.188, 26.757, 28.3, 29.819, 31.319, 32.801, 34.267,
		35.718, 37.156, 38.582, 39.997}
	df := min(m-1, 20)

	succeeds = table[df-1] > chiStatistic
	return
}

func TestLocation(t *testing.T) {
	var m, k, rounds uint

	m = 8
	k = 3
	rounds = 100000 // 15000000

	elements := make([][]byte, rounds)

	for x := uint(0); x < rounds; x++ {
		ctrlist := make([]uint8, 4)
		ctrlist[0] = uint8(x)
		ctrlist[1] = uint8(x >> 8)
		ctrlist[2] = uint8(x >> 16)
		ctrlist[3] = uint8(x >> 24)
		data := ctrlist
		elements[x] = data
	}

	succeeds := chiTestBloom(m, k, rounds, elements)
	assert.True(t, succeeds)
}

func TestCap(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	assert.Equal(t, f.arraySize, f.Cap(), "Cap() should match arraySize")
}

func TestK(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	assert.Equal(t, f.hashNum, f.numberOfHashFunctions(), "numberOfHashFunctions() should match hashNum")
}

func TestWriteToReadFrom(t *testing.T) {
	var b bytes.Buffer
	f := NewBloomFilter(1000, 4)
	_, err := f.WriteTo(&b)
	assert.NoError(t, err)

	g := NewBloomFilter(1000, 1)
	_, err = g.ReadFrom(&b)
	assert.NoError(t, err)

	assert.Equal(t, f.arraySize, g.arraySize, "arraySize mismatch")
	assert.Equal(t, f.hashNum, g.hashNum, "hashNum mismatch")
	assert.NotNil(t, g.bitVector)
	assert.True(t, g.bitVector.Equal(f.bitVector), "bitsets mismatch")

	_ = g.Test([]byte(""))
}

func TestEncodeDecodeGob(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	f.Add([]byte("one"))
	f.Add([]byte("two"))
	f.Add([]byte("three"))

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(f)
	assert.NoError(t, err)

	var g Filter
	err = gob.NewDecoder(&buf).Decode(&g)
	assert.NoError(t, err)

	assert.Equal(t, f.arraySize, g.arraySize)
	assert.Equal(t, f.hashNum, g.hashNum)
	assert.NotNil(t, g.bitVector)
	assert.True(t, g.bitVector.Equal(f.bitVector))
	assert.True(t, g.Test([]byte("one")))
	assert.True(t, g.Test([]byte("two")))
	assert.True(t, g.Test([]byte("three")))
}

func TestReadWriteBinary(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	var buf bytes.Buffer
	bytesWritten, err := f.WriteTo(&buf)
	assert.NoError(t, err)
	assert.Equal(t, int64(buf.Len()), bytesWritten, "written length mismatch")

	var g Filter
	bytesRead, err := g.ReadFrom(&buf)
	assert.NoError(t, err)
	assert.Equal(t, bytesWritten, bytesRead)
	assert.Equal(t, f.arraySize, g.arraySize)
	assert.Equal(t, f.hashNum, g.hashNum)
	assert.NotNil(t, g.bitVector)
	assert.True(t, g.bitVector.Equal(f.bitVector))
}

func BenchmarkEstimated(b *testing.B) {
	for n := uint(100000); n <= 100000; n *= 10 {
		for fp := 0.1; fp >= 0.0001; fp /= 10.0 {
			f := NewWithEstimates(n, fp)
			_ = EstimateFalsePositiveRate(f.arraySize, f.hashNum, n)
		}
	}
}

func BenchmarkSeparateTestAndAdd(b *testing.B) {
	f := NewWithEstimates(uint(b.N), 0.0001)
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.Test(key)
		f.Add(key)
	}
}

func BenchmarkCombinedTestAndAdd(b *testing.B) {
	f := NewWithEstimates(uint(b.N), 0.0001)
	key := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		f.TestAndAdd(key)
	}
}

func TestMerge(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	n1 := []byte("f")
	f.Add(n1)

	g := NewBloomFilter(1000, 4)
	n2 := []byte("g")
	g.Add(n2)

	h := NewBloomFilter(999, 4)
	n3 := []byte("h")
	h.Add(n3)

	j := NewBloomFilter(1000, 5)
	n4 := []byte("j")
	j.Add(n4)

	err := f.Merge(g)
	assert.NoError(t, err, "should merge filters with same arraySize and hashNum")

	err = f.Merge(h)
	assert.Error(t, err, "should fail merge due to different arraySize")

	err = f.Merge(j)
	assert.Error(t, err, "should fail merge due to different hashNum")

	assert.True(t, f.Test(n2), "n2 should exist after valid merge")
	assert.False(t, f.Test(n3), "n3 should not exist after invalid merge")
	assert.False(t, f.Test(n4), "n4 should not exist after invalid merge")
}

func TestEqual(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	f1 := NewBloomFilter(1000, 4)
	g := NewBloomFilter(1000, 20)
	h := NewBloomFilter(10, 20)
	n1 := []byte("Bess")
	f1.Add(n1)

	assert.True(t, f.Equal(f), "filter should equal itself")
	assert.False(t, f.Equal(f1), "should not equal a modified filter")
	assert.False(t, f.Equal(g), "should not equal filter with different hashNum")
	assert.False(t, f.Equal(h), "should not equal filter with different size")
}

func TestTestLocations(t *testing.T) {
	f := NewWithEstimates(1000, 0.001)
	n1 := []byte("Love")
	n2 := []byte("is")
	n3 := []byte("in")
	n4 := []byte("bloom")

	f.Add(n1)
	n3a := f.TestLocations(Locations(n3, f.numberOfHashFunctions()))
	f.Add(n3)
	n1b := f.TestLocations(Locations(n1, f.numberOfHashFunctions()))
	n2b := f.TestLocations(Locations(n2, f.numberOfHashFunctions()))
	n3b := f.TestLocations(Locations(n3, f.numberOfHashFunctions()))
	n4b := f.TestLocations(Locations(n4, f.numberOfHashFunctions()))

	assert.True(t, n1b, "n1 should be in")
	assert.False(t, n2b, "n2 should not be in")
	assert.False(t, n3a, "n3 should not be in first time")
	assert.True(t, n3b, "n3 should be in second time")
	assert.False(t, n4b, "n4 should not be in")
}

func TestApproximatedSize(t *testing.T) {
	f := NewWithEstimates(1000, 0.001)
	f.Add([]byte("Love"))
	f.Add([]byte("is"))
	f.Add([]byte("in"))
	f.Add([]byte("bloom"))

	assert.Equal(t, uint32(4), f.ApproximatedSize(), "approximated size should be 4")
}

func TestFPP(t *testing.T) {
	f := NewWithEstimates(1000, 0.001)
	for i := uint32(0); i < 1000; i++ {
		n := make([]byte, 4)
		binary.BigEndian.PutUint32(n, i)
		f.Add(n)
	}

	count := 0
	for i := uint32(0); i < 1000; i++ {
		n := make([]byte, 4)
		binary.BigEndian.PutUint32(n, i+1000)
		if f.Test(n) {
			count++
		}
	}

	actualFPP := float64(count) / 1000.0
	assert.LessOrEqual(t, actualFPP, 0.001, "excessive false positive probability")
}

func TestEncodeDecodeBinary(t *testing.T) {
	f := NewBloomFilter(1000, 4)
	f.Add([]byte("one"))
	f.Add([]byte("two"))
	f.Add([]byte("three"))

	data, err := f.MarshalBinary()
	assert.NoError(t, err)

	var g Filter
	err = g.UnmarshalBinary(data)
	assert.NoError(t, err)

	assert.Equal(t, f.arraySize, g.arraySize, "arraySize mismatch")
	assert.Equal(t, f.hashNum, g.hashNum, "hashNum mismatch")
	assert.NotNil(t, g.bitVector, "bitset should not be nil")
	assert.True(t, g.bitVector.Equal(f.bitVector), "bitsets should be equal")
	assert.True(t, g.Test([]byte("one")), "missing value 'one'")
	assert.True(t, g.Test([]byte("two")), "missing value 'two'")
	assert.True(t, g.Test([]byte("three")), "missing value 'three'")
}

func TestFilterEncodeDecode(t *testing.T) {
	// 1. 创建原始布隆过滤器
	original := NewBloomFilter(1024, 5)
	original.AddString("apple")
	original.AddString("banana")
	original.AddString("cherry")

	// 2. EncodeTo 序列化
	var buf bytes.Buffer
	err := original.EncodeTo(&buf)
	assert.NoError(t, err)

	// 3. DecodeFrom 反序列化
	decoded := &Filter{}
	err = decoded.DecodeFrom(&buf)
	assert.NoError(t, err)

	// 4. 测试结果是否一致
	assert.True(t, decoded.TestString("apple"))
	assert.True(t, decoded.TestString("banana"))
	assert.True(t, decoded.TestString("cherry"))
	assert.False(t, decoded.TestString("durian"))

	// 5. Equal 校验（可选）
	assert.True(t, original.Equal(decoded), "original and decoded filters should be equal")
}
