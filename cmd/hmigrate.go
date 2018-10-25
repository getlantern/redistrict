package cmd

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/spf13/cobra"

	pb "gopkg.in/cheggaaa/pb.v2"
)

// hmigrateCmd is for migrating a particular hash to a new redis.
var hmigrateCmd = &cobra.Command{
	Use:   "hmigrate",
	Short: "Migrate a large hash at the specified key",
	Long: `Redis DUMP, RESTORE, and MIGRATE commands do not support hashes larger than 512MB. This
uses HSCAN to migrate large hashes.`,
	Run: hmigrate,
}

var key string

func init() {
	rootCmd.AddCommand(hmigrateCmd)
	hmigrateCmd.Flags().StringVarP(&key, "key", "k", "", "The key of the hash to migrate")
	hmigrateCmd.MarkFlagRequired("key")
}

type hscan func(key string, cursor uint64, match string, count int64) *redis.ScanCmd

type hset func(key string, hmap map[string]interface{}) *redis.StatusCmd

type hlen func(key string) *redis.IntCmd

func hmigrate(cmd *cobra.Command, args []string) {
	hmigrateWith(sclient.HScan, dclient.HMSet, sclient.HLen)
}

func hmigrateWith(scan hscan, set hset, hl hlen) {
	fmt.Printf("hmigrate called with key %+v\n", key)
	length := hl(key).Val()
	var bar *pb.ProgressBar
	if length > 0 {
		bar = pb.StartNew(int(length))
	}

	var cursor uint64
	var n int64
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = scan(key, cursor, "", 100000).Result()
		if err != nil {
			panic(err)
		}
		cur := len(keyvals)
		n += int64(cur)

		if length > 0 && bar != nil {
			bar.Add(cur / 2)
		}

		hmap := keyvalsToMap(keyvals)
		status, err := set(key, hmap).Result()
		if err != nil {
			fmt.Printf("Error setting values on destination %v", err)
			fmt.Printf("Status: %v", status)
		}
		if cursor == 0 {
			break
		}
		time.Sleep(time.Millisecond * 2000)
	}
	if bar != nil {
		bar.Finish()
	}
}

func keyvalsToMap(keyvals []string) map[string]interface{} {
	// This is a little quirky. Redis scans return keys followed by values, so to create a map
	// for a subsequent hmset call we have to iterate one forward in the array to map the value.
	hmap := make(map[string]interface{})
	size := len(keyvals)
	if size == 0 {
		return hmap
	}
	i := 0
	for {
		hmap[keyvals[i]] = keyvals[i+1]
		if size == i+2 {
			break
		}
		i += 2
	}

	return hmap
}
