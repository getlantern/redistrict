package cmd

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/spf13/cobra"
	pb "gopkg.in/cheggaaa/pb.v1"
)

const compareCount = 4000

type differ struct {
	key      string
	allKeys1 map[string]bool
	allKeys2 map[string]bool
}

// diffCmd is for comparing two redis databases.
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Compares two redis databases",
	Long:  `Compares two redis databases using scans and pipelining`,
	Run:   diff,
}

func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.Flags().StringVarP(&cmdKey, "key", "k", "", "A specific key to compare, if any")
}

func newDiffer() *differ {
	return newDifferForKey("")
}

func newDifferForKey(key string) *differ {
	return &differ{
		key:      key,
		allKeys1: make(map[string]bool),
		allKeys2: make(map[string]bool),
	}
}

func diff(cmd *cobra.Command, args []string) {
	var d = newDifferForKey(cmdKey)
	d.diff()
}

func (d *differ) diff() bool {

	size1, err := sclient.DBSize().Result()
	if err != nil {
		panic(fmt.Sprintf("Error getting database 1 size: %v", err))
	}

	size2, err := dclient.DBSize().Result()
	if err != nil {
		panic(fmt.Sprintf("Error getting source database 2 size: %v", err))
	}

	ch1 := make(chan []string)
	ch2 := make(chan []string)

	keysOnly := size1 != size2

	go genericReadWithClose("", func(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
		return sclient.Scan(cursor, "", count).Result()
	}, ch1, compareCount, keysOnly)

	go genericReadWithClose("", func(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
		return dclient.Scan(cursor, "", count).Result()
	}, ch2, compareCount, keysOnly)

	var bar *pb.ProgressBar
	var differ bool
	if keysOnly {
		logger.Debugf("Databases have different sizes: %v and %v...determining differing keys", size1, size2)
		bar = pb.StartNew(int(size1) + int(size2))
		differ = d.diffKeys(ch1, ch2, size1, size2, bar)
	} else {
		bar = pb.StartNew(int(size1))
		differ = d.resultsDiffer(ch1, ch2, size1, bar)
	}

	bar.FinishPrint(fmt.Sprintf("The End! Redises are different: %v", differ))
	return differ
}

type ktv struct {
	key      string
	ttlCmd   *redis.DurationCmd
	valueCmd *redis.StringCmd
}

func (d *differ) diffKeys(ch1, ch2 chan []string, size1, size2 int64, bar *pb.ProgressBar) bool {
	var wg sync.WaitGroup
	wg.Add(2)

	go d.diffOnChan(ch1, d.allKeys1, d.allKeys2, bar, &wg, size1)
	go d.diffOnChan(ch2, d.allKeys2, d.allKeys1, bar, &wg, size2)
	wg.Wait()
	return d.checkKeys("DB1", d.allKeys1) || d.checkKeys("DB2", d.allKeys2)
}

func (d *differ) checkKeys(name string, keys map[string]bool) bool {
	if len(keys) > 0 {
		logger.Debugf("Remaining keys for %v: %+v", name, keys)
		return true
	}
	return false
}

func (d *differ) diffOnChan(ch chan []string, chanKeys, otherKeys map[string]bool,
	bar *pb.ProgressBar, wg *sync.WaitGroup, size int64) {
	if size != 0 {
		for keys := range ch {
			for _, key := range keys {
				if _, ok := otherKeys[key]; ok {
					// Delete matching keys as we go and don't add them to avoid consuming too much memory.
					delete(otherKeys, key)
				} else {
					chanKeys[key] = true
				}
			}
		}
	}

	wg.Done()
}

func (d *differ) resultsDiffer(ch1, ch2 chan []string, size int64, bar *pb.ProgressBar) bool {
	if size == 0 {
		return false
	}
	pipeline1 := sclient.Pipeline()
	pipeline2 := dclient.Pipeline()
	index := int64(0)
	for {
		keys1 := <-ch1
		keys2 := <-ch2
		bar.Increment()
		if len(keys1) != len(keys2) {
			logger.Debugf("Key length mismatch: %v, %v", len(keys1), len(keys2))
			return true
		}

		ktvs1 := d.ktvs(keys1, pipeline1)
		ktvs2 := d.ktvs(keys2, pipeline2)

		if d.ktvArraysDiffer(ktvs1, ktvs2) {
			return true
		}

		index++
		if index == size {
			break
		}
	}
	return false
}

func (d *differ) ktvArraysDiffer(vals1, vals2 []ktv) bool {
	if len(vals1) != len(vals2) {
		return true
	}
	size := len(vals1)

	for i := 0; i < size; i++ {
		a := vals1[i]
		b := vals2[i]
		if d.ktvsDiffer(a, b) {
			return true
		}
	}
	return false
}

func (d *differ) ktvsDiffer(a, b ktv) bool {
	if a.key != b.key {
		logger.Debugf("Keys differ: %v, %v", a.key, b.key)
		return true
	}

	if d.ttl(a) != d.ttl(b) {
		logger.Debugf("TTLs differ for key %v: %v != %v", b.key, d.ttl(a), d.ttl(b))
		return true
	}

	if d.val(a) != d.val(b) {
		logger.Debugf("Vals differ for key %v: %v != %v", b.key, d.val(a), d.val(b))
		return true
	}
	return false
}

func (d *differ) ttl(k ktv) time.Duration {
	ttl, err := k.ttlCmd.Result()
	if err != nil {
		panic(fmt.Sprintf("Error reading key %v: %v", k.key, err))
	}
	return ttl
}

func (d *differ) val(k ktv) string {
	val, err := k.valueCmd.Result()
	if err != nil {
		panic(fmt.Sprintf("Error reading value for key %v: %v", k.key, err))
	}
	return val
}

func (d *differ) ktvs(keys []string, pipe redis.Pipeliner) []ktv {
	ktvs := make([]ktv, 0)
	n := len(keys)
	for i := 0; i < n; i++ {
		key := keys[i]
		if _, ok := largeKeys[key]; ok {
			continue
		}

		ttlCmd := pipe.PTTL(key)
		dumpCmd := pipe.Dump(key)
		newKTV := ktv{key: key, ttlCmd: ttlCmd, valueCmd: dumpCmd}
		ktvs = append(ktvs, newKTV)
	}

	if _, err := pipe.Exec(); err != nil {
		panic(fmt.Sprintf("Error execing source pipeline: %v", err))
	}

	return ktvs
}
