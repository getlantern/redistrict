package cmd

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "gopkg.in/cheggaaa/pb.v1"
)

func TestDiffOnChan(t *testing.T) {
	ch := make(chan []string, 10)
	d := newDiffer()
	d.ignoreKeys = "code:.*"

	size := 0
	for i := 0; i < 10; i++ {
		keys := make([]string, 4)
		for j := 0; j < len(keys); j++ {
			keys[j] = "code:" + strconv.Itoa(i) + strconv.Itoa(j)
			size++
		}
		ch <- keys
	}

	chanKeys := new(sync.Map)
	otherKeys := new(sync.Map)

	bar := pb.StartNew(int(size))

	var wg sync.WaitGroup
	wg.Add(1)

	go d.diffOnChan(ch, chanKeys, otherKeys, bar, &wg, int64(size))
	close(ch)
	wg.Wait()
	chanKeyLen := mapLength(chanKeys)
	otherKeyLen := mapLength(otherKeys)
	assert.Equal(t, 0, chanKeyLen)
	assert.Equal(t, 0, otherKeyLen)
	assert.Equal(t, int64(size), bar.Get())

	ch1 := make(chan []string, 10)
	size = 0
	for i := 0; i < 10; i++ {
		keys := make([]string, 4)
		for j := 0; j < len(keys); j++ {
			keys[j] = "codenomatch" + strconv.Itoa(i) + strconv.Itoa(j)
			size++
		}
		ch1 <- keys
	}

	bar = pb.StartNew(int(size))

	var wg1 sync.WaitGroup
	wg1.Add(1)

	go d.diffOnChan(ch1, chanKeys, otherKeys, bar, &wg1, int64(size))

	close(ch1)
	wg1.Wait()

	chanKeyLen = mapLength(chanKeys)
	otherKeyLen = mapLength(otherKeys)
	assert.Equal(t, size, chanKeyLen)
	assert.Equal(t, 0, otherKeyLen)
	assert.Equal(t, int64(size), bar.Get())
}

func mapLength(mapped *sync.Map) int {
	length := 0
	mapped.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}

// TestFetchKTVs tests the fetchKTVs function.
func TestFetchKTVs(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	d := newDiffer()

	sclient.FlushAll()
	dclient.FlushAll()

	keys := make([]string, 0)
	testLength := 40
	for i := 0; i < testLength; i++ {
		key := fmt.Sprintf("key-%d", i)
		keys = append(keys, key)
		err := sclient.Set(key, fmt.Sprintf("value-%d", i), 0*time.Second).Err()
		if err != nil {
			panic(err)
		}
	}

	ktvs, diffDetected := d.fetchKTVs(keys, sclient)
	assert.False(t, diffDetected)
	assert.Equal(t, len(ktvs), len(keys))

	// Now query for keys that don't exist.
	keys = make([]string, 0)
	keys = append(keys, "doesnotexists48284")
	_, diffDetected = d.fetchKTVs(keys, sclient)
	assert.True(t, diffDetected)
	sclient.FlushAll()
}

// TestSameDB tests two equal databases.
func TestSameDB(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	d := newDiffer()

	sclient.FlushAll()
	dclient.FlushAll()

	keys := make([]string, 0)
	testLength := 40
	for i := 0; i < testLength; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		keys = append(keys, key)
		err := sclient.Set(key, val, 0).Err()
		if err != nil {
			panic(err)
		}
		err = dclient.Set(key, val, 0).Err()
		if err != nil {
			panic(err)
		}
	}
	assert.Equal(t, int64(testLength), sclient.DBSize().Val())
	assert.Equal(t, int64(testLength), dclient.DBSize().Val())

	diffDetected := d.diff()
	assert.False(t, diffDetected)

	dclient.FlushAll()
	sclient.FlushAll()
}

// TestValuesDiffer tests two databases with equal keys but different values.
func TestValuesDiffer(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	d := newDiffer()

	sclient.FlushAll()
	dclient.FlushAll()

	keys := make([]string, 0)
	testLength := 40
	for i := 0; i < testLength; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		keys = append(keys, key)
		assert.NoError(t, sclient.Set(key, val, 0).Err())
		assert.NoError(t, dclient.Set(key, val, 0).Err())
	}

	// Set one of the values to be different.
	assert.NoError(t, dclient.Set(fmt.Sprintf("key-%d", testLength/2), "value-different", 0).Err())
	assert.Equal(t, int64(testLength), sclient.DBSize().Val())
	assert.Equal(t, int64(testLength), dclient.DBSize().Val())

	diffDetected := d.diff()
	assert.True(t, diffDetected)

	dclient.FlushAll()
	sclient.FlushAll()
}

// TestKeysDiffer tests two databases with equal values but different keys.
func TestKeysDiffer(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	d := newDiffer()

	sclient.FlushAll()
	dclient.FlushAll()

	keys := make([]string, 0)
	testLength := 40
	for i := 0; i < testLength-1; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		keys = append(keys, key)
		assert.NoError(t, sclient.Set(key, val, 0).Err())
		assert.NoError(t, dclient.Set(key, val, 0).Err())
	}

	// Set one of the keys to be different.
	assert.NoError(t, sclient.Set(fmt.Sprintf("key-%d", testLength+1), "val", 0).Err())
	assert.NoError(t, dclient.Set(fmt.Sprintf("key-%d", testLength+20), "val", 0).Err())
	assert.Equal(t, int64(testLength), sclient.DBSize().Val())
	assert.Equal(t, int64(testLength), dclient.DBSize().Val())

	diffDetected := d.diff()
	assert.True(t, diffDetected)

	dclient.FlushAll()
	sclient.FlushAll()
}

// TestExtraKey tests adding an extra key to one database.
func TestExtraKey(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	d := newDiffer()

	sclient.FlushAll()
	dclient.FlushAll()

	keys := make([]string, 0)
	testLength := 40
	for i := 0; i < testLength; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		keys = append(keys, key)
		err := sclient.Set(key, val, 0).Err()
		if err != nil {
			panic(err)
		}
		err = dclient.Set(key, val, 0).Err()
		if err != nil {
			panic(err)
		}
	}
	assert.NoError(t, dclient.Set("newkey", "newval", 0).Err())

	diffDetected := d.diff()
	assert.True(t, diffDetected)

	// Now make the first db larger
	assert.NoError(t, sclient.Set("newkey", "newval", 0).Err())
	assert.NoError(t, sclient.Set("newkey1", "newval1", 0).Err())

	diffDetected = d.diff()
	assert.True(t, diffDetected)

	dclient.FlushAll()
	sclient.FlushAll()
}

// TestDiff tests the diff function.
func TestDiff(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	d := newDiffer()

	sclient.FlushAll()
	dclient.FlushAll()

	testkey := "list1"
	testLength := 40
	for i := 0; i < testLength; i++ {
		err := sclient.RPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}

	assert.True(t, d.diff())

	// Now insert the same key but different value to test that.
	for i := 0; i < 2; i++ {
		err := dclient.RPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}

	assert.True(t, d.diff())
	dclient.FlushAll()

	// Now test what should be the same data.
	for i := 0; i < testLength; i++ {
		err := dclient.RPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	assert.False(t, d.diff())
	dclient.FlushAll()

	// Now try adding a key with a different name but the same values.
	testkey = "different"
	for i := 0; i < testLength; i++ {
		err := dclient.RPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	assert.True(t, d.diff())
	dclient.FlushAll()

	// Now make the second DB larger than the first.
	testkey = "list1"
	for i := 0; i < testLength; i++ {
		err := dclient.RPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	dclient.Set("newkey", "newkey", 10*time.Hour)
	assert.True(t, d.diff())

	sclient.FlushAll()
	dclient.FlushAll()

	logger.Debug("Testing two empty DBs")
	// Test two empty DBs!
	assert.False(t, d.diff())
}
