package cmd

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestMigrate tests migration from one database to another.
func TestMigrate(t *testing.T) {
	flushdst = true
	dst = "127.0.0.1:7777"
	initRedis()

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

	migrate(nil, make([]string, 0))

	for i := 0; i < 20; i++ {
		cmd := dclient.Get(fmt.Sprintf("key-%d", i))
		val, err := cmd.Result()
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
	}
}
