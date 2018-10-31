package cmd

import (
	"fmt"
	"sync"

	"github.com/go-redis/redis"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	pb "gopkg.in/cheggaaa/pb.v2"
)

type hmigrator struct {
	key    string
	hcount int
}

var hm = &hmigrator{}

// hmigrateCmd is for migrating a particular hash to a new redis.
var hmigrateCmd = &cobra.Command{
	Use:   "hmigrate",
	Short: "Migrate a large hash at the specified key",
	Long: `Redis DUMP, RESTORE, and MIGRATE commands do not support hashes larger than 512MB. This
uses HSCAN to migrate large hashes. This is essentially akin to a theoretical HMIGRATE redis
command.`,
	Run: hm.hmigrate,
}

func init() {
	rootCmd.AddCommand(hmigrateCmd)
	hmigrateCmd.Flags().StringVarP(&hm.key, "key", "k", "", "The key of the hash to migrate")
	hmigrateCmd.MarkFlagRequired("key")
	hmigrateCmd.Flags().IntVarP(&hm.hcount, "hcount", "", 5000, "The number of hash entries to scan on each pass")
}

type hscan func(key string, cursor uint64, match string, count int64) *redis.ScanCmd

type hset func(key string, hmap map[string]interface{}) *redis.StatusCmd

type hlen func(key string) *redis.IntCmd

func hmigrateKey(k string, bar *mpb.Bar, wg *sync.WaitGroup) {
	hm.key = k
	hm.migrate(bar, wg)
}

func (hm *hmigrator) migrate(bar *mpb.Bar, wg *sync.WaitGroup) {
	wg.Add(1)
	hm.hmigrateWith(sclient.HScan, dclient.HMSet, sclient.HLen, bar, wg)
}

func (hm *hmigrator) hmigrate(cmd *cobra.Command, args []string) {
	// This is a dummy waitgroup. The waitgroup is really only used when migrating large hashes as
	// a part of a larger migration.
	var wg sync.WaitGroup
	wg.Add(1)
	hm.migrate(nil, &wg)
}

type progress interface {
	Finish() *pb.ProgressBar
	Add(int) *pb.ProgressBar
}

type prog struct{}

func (p *prog) Finish() *pb.ProgressBar { return nil }
func (p *prog) Add(int) *pb.ProgressBar { return nil }

func (hm *hmigrator) hmigrateWith(scan hscan, set hset, hl hlen, bar *mpb.Bar, wg *sync.WaitGroup) {
	length, err := hl(hm.key).Result()
	if err != nil {
		panic(fmt.Sprintf("Could not get hash length %v", err))
	}
	if bar == nil {
		p := mpb.New()
		bar = p.AddBar(length)
	}

	ch := make(chan map[string]interface{})

	go hm.read(scan, ch)
	hm.write(hm.key, set, ch, bar, wg)
}

func (hm *hmigrator) read(scan hscan, ch chan map[string]interface{}) {
	var cursor uint64
	var n int64
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = scan(hm.key, cursor, "", int64(hm.hcount)).Result()
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

func (hm *hmigrator) write(key string, set hset, ch chan map[string]interface{}, bar *mpb.Bar, wg *sync.WaitGroup) {
	defer wg.Done()
	for hmap := range ch {
		status, err := set(key, hmap).Result()
		if err != nil {
			fmt.Printf("Error setting values on destination %v", err)
			fmt.Printf("Status: %v", status)
		} else {
			bar.IncrBy(len(hmap))
		}
	}
	//bar.Finish()
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
