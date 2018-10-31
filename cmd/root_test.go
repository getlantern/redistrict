package cmd

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/spf13/cobra/doc"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	var m = &migrator{
		src:         "127.0.0.1:6379",
		dst:         "127.0.0.1:6379",
		largeHashes: make(map[string]bool),
		tempHashes:  make([]string, 0),
	}
	m.cfgFile = ""
	m.initConfig()

	empty := viper.GetStringSlice("does-not-exist")
	assert.Equal(t, 0, len(empty))

	err := doc.GenMarkdownTree(rootCmd, "")
	if err != nil {
		log.Fatal(err)
	}

	err = doc.GenMarkdownTree(hmigrateCmd, "")
	if err != nil {
		log.Fatal(err)
	}
}

func TestRootMigrate(t *testing.T) {
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
