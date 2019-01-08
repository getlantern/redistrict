package cmd

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	pb "gopkg.in/cheggaaa/pb.v1"
)

func TestConfig(t *testing.T) {
	var m = newMigrator()
	m.cfgFile = ""
	m.initConfig()

	empty := viper.GetStringSlice("does-not-exist")
	assert.Equal(t, 0, len(empty))

	/*
		doc.GenMarkdownTree(rootCmd, "")
		doc.GenMarkdownTree(hmigrateCmd, "")
		doc.GenMarkdownTree(lmigrateCmd, "")
		doc.GenMarkdownTree(smigrateCmd, "")
	*/
}

var testMigFunc = func(k string, wg *sync.WaitGroup, pool *pb.Pool) int {
	return 1
}

func TestIntegrateConfigSettings(t *testing.T) {

	keys := []string{"1", "2", "3"}
	kmap := map[string]migFunc{"4": testMigFunc}

	migr := newMigrator()
	largeKeys = kmap
	migr.integrateConfigSettings(keys, testMigFunc)
	assert.Equal(t, 4, len(kmap))
	assert.NotNil(t, kmap["1"])
	assert.NotNil(t, kmap["3"])
	assert.Nil(t, kmap["5"])
}

func TestPopulateKeyMap(t *testing.T) {

	keys := []string{"1", "2", "3"}
	kmap := map[string]migFunc{}

	migr := newMigrator()
	largeKeys = kmap

	migr.populateKeyMapFrom("keysName", func(arg1 string) []string {
		return keys
	}, testMigFunc)

	assert.NotNil(t, kmap["1"])
	assert.NotNil(t, kmap["3"])
	assert.Nil(t, kmap["4"])
}

func TestRootMigrate(t *testing.T) {
	var m = newMigrator()
	m.flushdst = true
	m.flushsrc = true

	// Just use a separate database on the single redis instance.
	m.dstdb = 1
	m.initRedis()

	for i := 0; i < 20; i++ {
		err := sclient.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), 1*time.Hour).Err()
		if err != nil {
			panic(err)
		}
	}

	// Make sure the values aren't there before migrating.
	for i := 0; i < 20; i++ {
		cmd := dclient.Get(fmt.Sprintf("key-%d", i))
		_, err := cmd.Result()
		assert.Error(t, err)
	}

	m.migrate(nil, make([]string, 0))

	for i := 0; i < 20; i++ {
		cmd := dclient.Get(fmt.Sprintf("key-%d", i))
		val, err := cmd.Result()
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
	}
}
