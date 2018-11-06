package cmd

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLMigrate tests the lmigrate method. Note you either need to run this inside circleci, which
// creates a redis instance inside docker, or you need to have a locally running redis on the
// default port.
func TestLMigrate(t *testing.T) {
	var m = &migrator{
		src:         "127.0.0.1:6379",
		dst:         "127.0.0.1:6379",
		largeHashes: make(map[string]bool),
		tempHashes:  make([]string, 0),
	}

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	testkey := "list1"
	testLength := 1000
	for i := 0; i < testLength; i++ {
		err := sclient.LPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	cmdKey = testkey
	var wg sync.WaitGroup
	var lm = &lmigrator{key: cmdKey}
	lm.migrate(nil, &wg)

	logger.Debugf("Migrated test list...%v", dclient.LLen(testkey).Val())

	assert.Equal(t, int64(testLength), dclient.LLen(testkey).Val())
	logger.Debug("Popping all values...")
	for i := 0; i < testLength; i++ {
		get := dclient.LPop(testkey)
		val, err := get.Result()
		if err != nil {
			panic(err)
		}
		assert.True(t, strings.HasPrefix(val, "value-"))
	}

	assert.Equal(t, int64(0), dclient.LLen(testkey).Val())

	sclient.FlushAll()
	dclient.FlushAll()
}
