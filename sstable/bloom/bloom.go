// This code if copy from https://github.com/bits-and-blooms/bloom/blob/master/bloom.go
// Based on BSD-2-Clause license, the copyright notice is as follows:
/*
Package bloom provides data structures and methods for creating Bloom filters.

A Bloom filter is a representation of a set of _n_ items, where the main
requirement is to make membership queries; _i.e._, whether an item is a
member of a set.

A Bloom filter has two parameters: _m_, a maximum size (typically a reasonably large
multiple of the cardinality of the set to represent) and _k_, the number of hashing
functions on elements of the set. (The actual hashing functions are important, too,
but this is not a parameter for this implementation). A Bloom filter is backed by
a BitSet; a key is represented in the filter by setting the bits at each value of the
hashing functions (modulo _m_). Set membership is done by _testing_ whether the
bits at each value of the hashing functions (again, modulo _m_) are set. If so,
the item is in the set. If the item is actually in the set, a Bloom filter will
never fail (the true positive rate is 1.0); but it is susceptible to false
positives. The art is to choose _k_ and _m_ correctly.

In this implementation, the hashing functions used is murmurhash,
a non-cryptographic hashing function.

This implementation accepts keys for setting as testing as []byte. Thus, to
add a string item, "Love":

	uint n = 1000
	filter := bloom.NewBloomFilter(20*n, 5) // load of 20, 5 keys
	filter.Add([]byte("Love"))

Similarly, to test if "Love" is in bloom:

	if filter.Test([]byte("Love"))

For numeric data, I recommend that you look into the binary/encoding library. But,
for example, to add a uint32 to the filter:

	i := uint32(100)
	n1 := make([]byte,4)
	binary.BigEndian.PutUint32(n1,i)
	f.Add(n1)

Finally, there is a method to estimate the false positive rate of a
Bloom filter with _m_ bits and _k_ hashing functions for a set of size _n_:

	if bloom.EstimateFalsePositiveRate(20*n, 5, n) > 0.001 ...

You can use it to validate the computed arraySize, hashNum parameters:

	arraySize, hashNum := bloom.EstimateParameters(n, fp)
	ActualfpRate := bloom.EstimateFalsePositiveRate(arraySize, hashNum, n)

or

	f := bloom.NewWithEstimates(n, fp)
	ActualfpRate := bloom.EstimateFalsePositiveRate(f.arraySize, f.hashNum, n)

You would expect ActualfpRate to be close to the desired fp in these cases.

The EstimateFalsePositiveRate function creates a temporary Bloom filter. It is
also relatively expensive and only meant for validation.
*/

package bloom

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/bits-and-blooms/bitset"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

const (
	defaultBloomFilterM = 1600000
	defaultBloomFilterK = 16
)

// A Filter is a representation of a set of _n_ items, where the main
// requirement is to make membership queries; _i.e._, whether an item is a
// member of a set.
type Filter struct {
	arraySize uint           // 位图长度（bit array size）
	hashNum   uint           // 哈希函数个数（number of hash functions）
	bitVector *bitset.BitSet // 实际的位图数据（bit array）
}

// NewBloomFilter creates a new Bloom filter with _m_ bits and _k_ hashing functions
// We force _m_ and _k_ to be at least one to avoid panics.
func NewBloomFilter(m uint, k uint) *Filter {
	return &Filter{
		arraySize: max(1, m),
		hashNum:   max(1, k),
		bitVector: bitset.New(m),
	}
}

func DefaultBloomFilter() *Filter {
	return NewBloomFilter(defaultBloomFilterM, defaultBloomFilterK)
}

// From creates a new Bloom filter with len(_data_) * 64 bits and _k_ hashing
// functions. The data slice is not going to be reset.
func From(data []uint64, k uint) *Filter {
	m := uint(len(data) * 64)
	return FromWithM(data, m, k)
}

// FromWithM creates a new Bloom filter with _m_ length, _k_ hashing functions.
// The data slice is not going to be reset.
func FromWithM(data []uint64, m, k uint) *Filter {
	return &Filter{
		arraySize: m,
		hashNum:   k,
		bitVector: bitset.From(data),
	}
}

// baseHashes returns the four hash values of data that are used to create hashNum
// hashes
func baseHashes(data []byte) [4]uint64 {
	var d digest128 // murmur hashing
	hash1, hash2, hash3, hash4 := d.sum256(data)
	return [4]uint64{hash1, hash2, hash3, hash4}
}

// location returns the ith hashed location using the four base hash values
func location(h [4]uint64, i uint) uint64 {
	ii := uint64(i)
	return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
}

// location returns the ith hashed location using the four base hash values
func (f *Filter) location(h [4]uint64, i uint) uint {
	return uint(location(h, i) % uint64(f.arraySize))
}

// EstimateParameters estimates requirements for arraySize and hashNum.
// Based on https://bitbucket.org/ww/bloom/src/829aa19d01d9/bloom.go
// used with permission.
func EstimateParameters(n uint, p float64) (m uint, k uint) {
	m = uint(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = uint(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return
}

// NewWithEstimates creates a new Bloom filter for about n items with fp
// false positive rate
func NewWithEstimates(n uint, fp float64) *Filter {
	m, k := EstimateParameters(n, fp)
	return NewBloomFilter(m, k)
}

// Cap returns the capacity, _m_, of a Bloom filter
func (f *Filter) Cap() uint {
	return f.arraySize
}

// numberOfHashFunctions returns the number of hash functions used in the Filter
func (f *Filter) numberOfHashFunctions() uint {
	return f.hashNum
}

// BitSet returns the underlying bitset for this filter.
func (f *Filter) BitSet() *bitset.BitSet {
	return f.bitVector
}

// Add data to the Bloom Filter. Returns the filter (allows chaining)
func (f *Filter) Add(data []byte) *Filter {
	h := baseHashes(data)
	for i := uint(0); i < f.hashNum; i++ {
		f.bitVector.Set(f.location(h, i))
	}
	return f
}

// Merge the data from two Bloom Filters.
func (f *Filter) Merge(g *Filter) error {
	// Make sure the arraySize's and hashNum's are the same, otherwise merging has no real use.
	if f.arraySize != g.arraySize {
		return fmt.Errorf("arraySize's don't match: %d != %d", f.arraySize, g.arraySize)
	}

	if f.hashNum != g.hashNum {
		return fmt.Errorf("hashNum's don't match: %d != %d", f.arraySize, g.arraySize)
	}

	f.bitVector.InPlaceUnion(g.bitVector)
	return nil
}

// Copy creates a copy of a Bloom filter.
func (f *Filter) Copy() *Filter {
	fc := NewBloomFilter(f.arraySize, f.hashNum)
	err := fc.Merge(f)
	if err != nil {
		return nil
	}

	return fc
}

// AddString to the Bloom Filter. Returns the filter (allows chaining)
func (f *Filter) AddString(data string) *Filter {
	return f.Add([]byte(data))
}

// ClearAll clears all the data in a Bloom filter, removing all keys
func (f *Filter) ClearAll() *Filter {
	f.bitVector.ClearAll()
	return f
}

// ApproximatedSize approximating the number of items
// https://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
func (f *Filter) ApproximatedSize() uint32 {
	x := float64(f.bitVector.Count())
	m := float64(f.Cap())
	k := float64(f.numberOfHashFunctions())
	size := -1 * m / k * math.Log(1-x/m) / math.Log(math.E)
	return uint32(math.Floor(size + 0.5)) // round
}

// WriteTo writes a binary representation of the Filter to an i/o stream.
// It returns the number of bytes written.
//
// Performance: if this function is used to write to a disk or network
// connection, it might be beneficial to wrap the stream in a bufio.Writer.
// E.g.,
//
//	      f, err := os.Create("myfile")
//		       w := bufio.NewWriter(f)
func (f *Filter) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(f.arraySize))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(f.hashNum))
	if err != nil {
		return 0, err
	}
	numBytes, err := f.bitVector.WriteTo(stream)
	return numBytes + int64(2*binary.Size(uint64(0))), err
}

// ReadFrom reads a binary representation of the Filter (such as might
// have been written by WriteTo()) from an i/o stream. It returns the number
// of bytes read.
//
// Performance: if this function is used to read from a disk or network
// connection, it might be beneficial to wrap the stream in a bufio.Reader.
// E.g.,
//
//	f, err := os.Open("myfile")
//	r := bufio.NewReader(f)
func (f *Filter) ReadFrom(stream io.Reader) (int64, error) {
	var m, k uint64
	err := binary.Read(stream, binary.BigEndian, &m)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &k)
	if err != nil {
		return 0, err
	}
	b := &bitset.BitSet{}
	numBytes, err := b.ReadFrom(stream)
	if err != nil {
		return 0, err
	}
	f.arraySize = uint(m)
	f.hashNum = uint(k)
	f.bitVector = b
	return numBytes + int64(2*binary.Size(uint64(0))), nil
}

// GobEncode implements gob.GobEncoder interface.
func (f *Filter) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	_, err := f.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// GobDecode implements gob.GobDecoder interface.
func (f *Filter) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	_, err := f.ReadFrom(buf)

	return err
}

// MarshalBinary implements binary.BinaryMarshaler interface.
func (f *Filter) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	_, err := f.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary implements binary.BinaryUnmarshaler interface.
func (f *Filter) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	_, err := f.ReadFrom(buf)

	return err
}

// Equal tests for the equality of two Bloom filters
func (f *Filter) Equal(g *Filter) bool {
	return f.arraySize == g.arraySize && f.hashNum == g.hashNum && f.bitVector.Equal(g.bitVector)
}

// Locations returns a list of hash locations representing a data item.
func Locations(data []byte, k uint) []uint64 {
	locs := make([]uint64, k)

	// calculate locations
	h := baseHashes(data)
	for i := uint(0); i < k; i++ {
		locs[i] = location(h, i)
	}

	return locs
}

// EstimateFalsePositiveRate returns, for a Filter of arraySize bits
// and hashNum hash functions, an estimation of the false positive rate when
//
//	storing n entries. This is an empirical, relatively slow
//
// test using integers as keys.
// This function is useful to validate the implementation.
func EstimateFalsePositiveRate(m, k, n uint) (fpRate float64) {
	rounds := uint32(100000)
	// We construct a new filter.
	f := NewBloomFilter(m, k)
	n1 := make([]byte, 4)
	// We populate the filter with n values.
	for i := uint32(0); i < uint32(n); i++ {
		binary.BigEndian.PutUint32(n1, i)
		f.Add(n1)
	}
	fp := 0
	// test for number of rounds
	for i := uint32(0); i < rounds; i++ {
		binary.BigEndian.PutUint32(n1, i+uint32(n)+1)
		if f.Test(n1) {
			fp++
		}
	}
	fpRate = float64(fp) / (float64(rounds))
	return
}

// Test returns true if the data is in the Filter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (f *Filter) Test(data []byte) bool {
	h := baseHashes(data)
	for i := uint(0); i < f.hashNum; i++ {
		if !f.bitVector.Test(f.location(h, i)) {
			return false
		}
	}
	return true
}

// TestString returns true if the string is in the Filter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (f *Filter) TestString(data string) bool {
	return f.Test([]byte(data))
}

// TestLocations returns true if all locations are set in the Filter, false
// otherwise.
func (f *Filter) TestLocations(locs []uint64) bool {
	for i := 0; i < len(locs); i++ {
		if !f.bitVector.Test(uint(locs[i] % uint64(f.arraySize))) {
			return false
		}
	}
	return true
}

// TestAndAdd is equivalent to calling Test(data) then Add(data).
// The filter is written to unconditionnally: even if the element is present,
// the corresponding bits are still set. See also TestOrAdd.
// Returns the result of Test.
func (f *Filter) TestAndAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < f.hashNum; i++ {
		l := f.location(h, i)
		if !f.bitVector.Test(l) {
			present = false
		}
		f.bitVector.Set(l)
	}
	return present
}

// TestAndAddString is the equivalent to calling Test(string) then Add(string).
// The filter is written to unconditionally: even if the string is present,
// the corresponding bits are still set. See also TestOrAdd.
// Returns the result of Test.
func (f *Filter) TestAndAddString(data string) bool {
	return f.TestAndAdd([]byte(data))
}

// TestOrAdd is equivalent to calling Test(data) then if not present Add(data).
// If the element is already in the filter, then the filter is unchanged.
// Returns the result of Test.
func (f *Filter) TestOrAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < f.hashNum; i++ {
		l := f.location(h, i)
		if !f.bitVector.Test(l) {
			present = false
			f.bitVector.Set(l)
		}
	}
	return present
}

// TestOrAddString is the equivalent to calling Test(string) then if not present Add(string).
// If the string is already in the filter, then the filter is unchanged.
// Returns the result of Test.
func (f *Filter) TestOrAddString(data string) bool {
	return f.TestOrAdd([]byte(data))
}

// MayContain for SSTable key
func (f *Filter) MayContain(key kv.Key) bool {
	return f.TestString(string(key))
}

// DecodeFrom 从 io.Reader 解码 Filter（小端存储 + uint64 长度前缀）
func (f *Filter) DecodeFrom(r io.Reader) error {
	// 1. 读取长度字段（8字节小端）
	var length uint64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		log.Errorf("read filter length failed: %s", err)
		return fmt.Errorf("decode filter length: %w", err)
	}

	// 2. 根据长度读取二进制数据
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		log.Errorf("read filter data failed: %s", err)
		return fmt.Errorf("decode filter data: %w", err)
	}

	return f.UnmarshalBinary(data)
}

// EncodeTo 将 Filter 编码为字节流（小端存储 + uint64 长度前缀）
func (f *Filter) EncodeTo(w io.Writer) error {
	data, err := f.MarshalBinary()
	if err != nil {
		log.Errorf("marshal filter to binary failed: %s", err)
		return fmt.Errorf("encode filter: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(data))); err != nil {
		log.Errorf("write filter length failed: %s", err)
		return fmt.Errorf("encode filter length: %w", err)
	}

	// 3. 写入二进制数据
	_, err = w.Write(data)
	if err != nil {
		log.Errorf("marshal filter block failed: %s", err.Error())
		return fmt.Errorf("encode filter: %w", err)
	}

	return nil
}
