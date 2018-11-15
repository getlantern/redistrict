package cmd

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLMigrate tests the lmigrate method. Note you either need to run this inside circleci, which
// creates a redis instance inside docker, or you need to have a locally running redis on the
// default port.
func TestLMigrate(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	testkey := "list1"
	testLength := 40
	for i := 0; i < testLength; i++ {
		err := sclient.LPush(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	cmdKey = testkey
	var wg sync.WaitGroup
	var lm = &lmigrator{key: cmdKey}
	lcount = 7

	lm.migrate(&wg, nil)

	logger.Debugf("Migrated test list...%v", dclient.LLen(testkey).Val())

	assert.Equal(t, int64(testLength), dclient.LLen(testkey).Val())
	logger.Debug("Popping all values...")
	for i := 0; i < testLength; i++ {
		get := dclient.LPop(testkey)
		val, err := get.Result()
		if err != nil {
			panic(err)
		}

		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
	}

	assert.Equal(t, int64(0), dclient.LLen(testkey).Val())

	sclient.FlushAll()
	dclient.FlushAll()
}
