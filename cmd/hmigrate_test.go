package cmd

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestHMigrate tests the hmigrate method. Note you either need to run this inside circleci, which
// creates a redis instance inside docker, or you need to have a locally running redis on the
// default port.
func TestHMigrate(t *testing.T) {
	var m = newMigrator()

	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	testkey := "hkey1"
	for i := 0; i < 10000; i++ {
		err := sclient.HSet(testkey, fmt.Sprintf("field-%d", i), fmt.Sprintf("value-%d", i)).Err()
		if err != nil {
			panic(err)
		}
	}
	cmdKey = testkey
	var wg sync.WaitGroup
	var hm = &hmigrator{key: cmdKey}
	hm.migrate(&wg, dummyProgressPool)

	for i := 0; i < 10000; i++ {
		//logger.Debugf("dclient %v", dclient)
		get := dclient.HGet(testkey, fmt.Sprintf("field-%d", i))
		//logger.Debugf("get '%v'", get)
		val, err := get.Result()
		if err != nil {
			panic(err)
		}
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
	}
}

func TestKeyvalsToMap(t *testing.T) {
	keyvals := make([]string, 0)
	keyvalsMap := make(map[string]interface{})
	for i := 0; i < 10; i++ {
		keyvalsMap[strconv.Itoa(i)] = "val"
	}

	for i := 0; i < 10; i++ {
		keyvals = append(keyvals, strconv.Itoa(i))
		keyvals = append(keyvals, "val")
	}

	hm := &hmigrator{}
	mapped := hm.keyvalsToMap(keyvals)
	assert.Equal(t, keyvalsMap, mapped)
}
