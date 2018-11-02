package cmd

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSMigrate tests the smigrate method. Note you either need to run this inside circleci, which
// creates a redis instance inside docker, or you need to have a locally running redis on the
// default port.
func TestSMigrate(t *testing.T) {
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

	testkey := "set1"
	for i := 0; i < 10000; i++ {
		err := sclient.SAdd(testkey, fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	cmdKey = testkey
	var wg sync.WaitGroup
	var hm = &smigrator{key: cmdKey}
	hm.migrate(nil, &wg)

	vals, err := dclient.SMembers(testkey).Result()
	assert.NoError(t, err)
	assert.Equal(t, 10000, len(vals))
	for i := 0; i < len(vals); i++ {
		get := dclient.SPop(testkey)
		val, err := get.Result()
		if err != nil {
			panic(err)
		}
		assert.True(t, strings.HasPrefix(val, "value-"))
	}
}
