package cmd

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
	dclient.Del(testkey)

	// Now test what should be the same data.
	for i := 0; i < testLength; i++ {
		err := dclient.RPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	assert.False(t, d.diff())
	dclient.Del(testkey)

	// Now try adding a key with a different name but the same values.
	testkey = "different"
	for i := 0; i < testLength; i++ {
		err := dclient.RPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	assert.True(t, d.diff())
	dclient.Del(testkey)

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

	dclient.Del(testkey)
	sclient.FlushAll()
	dclient.FlushAll()

	logger.Debug("Testing two empty DBs")
	// Test two empty DBs!
	assert.False(t, d.diff())
}
