package cmd

import (
	"strconv"
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func TestHMigrate(t *testing.T) {
	scan := func(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
		return &redis.ScanCmd{}
	}
	set := func(key string, hmap map[string]interface{}) *redis.StatusCmd {
		return &redis.StatusCmd{}
	}
	hmigrateWith(scan, set)
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

	mapped := keyvalsToMap(keyvals)
	assert.Equal(t, keyvalsMap, mapped)
}
