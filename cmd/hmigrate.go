package cmd

import (
	"fmt"

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

type hmigrator struct {
	key   string
	count int
}

var hm = &hmigrator{}

func init() {
	rootCmd.AddCommand(hmigrateCmd)
	hmigrateCmd.Flags().StringVarP(&hm.key, "key", "k", "", "The key of the hash to migrate")
	hmigrateCmd.MarkFlagRequired("key")
	hmigrateCmd.Flags().IntVarP(&hm.count, "count", "", 1000, "The number of hash entries to scan on each pass")
}

type hscan func(key string, cursor uint64, match string, count int64) *redis.ScanCmd

type hset func(key string, hmap map[string]interface{}) *redis.StatusCmd

type hlen func(key string) *redis.IntCmd

func hmigrate(cmd *cobra.Command, args []string) {

	hm.hmigrateWith(sclient.HScan, dclient.HMSet, sclient.HLen)
}

func (hm *hmigrator) hmigrateWith(scan hscan, set hset, hl hlen) {
	length := hl(hm.key).Val()
	bar := pb.StartNew(int(length))

	ch := make(chan map[string]interface{})

	go hm.read(scan, ch)
	hm.write(hm.key, set, ch, bar)
}

func (hm *hmigrator) read(scan hscan, ch chan map[string]interface{}) {
	var cursor uint64
	var n int64
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = scan(hm.key, cursor, "", int64(hm.count)).Result()
		if err != nil {
			panic(err)
		}
		cur := len(keyvals)
		n += int64(cur)

		hmap := hm.keyvalsToMap(keyvals)
		ch <- hmap

		if cursor == 0 {
			close(ch)
			break
		}
	}
}

func (hm *hmigrator) write(key string, set hset, ch chan map[string]interface{}, bar *pb.ProgressBar) {
	for hmap := range ch {
		status, err := set(key, hmap).Result()
		if err != nil {
			fmt.Printf("Error setting values on destination %v", err)
			fmt.Printf("Status: %v", status)
		} else {
			bar.Add(len(hmap))
		}
	}
	bar.Finish()
}

func (hm *hmigrator) keyvalsToMap(keyvals []string) map[string]interface{} {
	// This is a little quirky. Redis scans return keys followed by values, so to create a map
	// for a subsequent set call we have to iterate one forward in the array to map the value.
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
