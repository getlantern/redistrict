package cmd

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/spf13/cobra"
	pb "gopkg.in/cheggaaa/pb.v1"
)

const compareCount = 4000

type differ struct {
	allKeys1   *sync.Map
	allKeys2   *sync.Map
	ignoreKeys string
}

var d = newDiffer()

// diffCmd is for comparing two redis databases.
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Compares two redis databases",
	Long:  `Compares two redis databases using scans and pipelining`,
	Run:   diff,
}

func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.Flags().StringVarP(&d.ignoreKeys, "ignorekeys", "", "", "Regex for keys to ignore")
}

func newDiffer() *differ {
	return &differ{
		allKeys1: new(sync.Map),
		allKeys2: new(sync.Map),
	}
}

func diff(cmd *cobra.Command, args []string) {
	var d = newDiffer()
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

	keysOnly := size1 != size2

	go genericReadWithClose("", func(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
		return sclient.Scan(cursor, "", count).Result()
	}, ch1, compareCount, keysOnly)

	var bar *pb.ProgressBar
	var differ bool
	if keysOnly {
		logger.Debugf("Databases have different sizes: %v and %v...determining differing keys", size1, size2)
		ch2 := make(chan []string)
		go genericReadWithClose("", func(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
			return dclient.Scan(cursor, "", count).Result()
		}, ch2, compareCount, keysOnly)
		bar = pb.StartNew(int(size1) + int(size2))
		differ = d.diffKeys(ch1, ch2, size1, size2, bar)
	} else {
		bar = pb.StartNew(int(size1))
		differ = d.resultsDiffer(ch1, size1, bar)
	}

	bar.FinishPrint(fmt.Sprintf("The End! Redises are different: %v", differ))
	return differ
}

type ktv struct {
	key string
	val string
	ttl time.Duration
}

func (d *differ) diffKeys(ch1, ch2 chan []string, size1, size2 int64, bar *pb.ProgressBar) bool {
	var wg sync.WaitGroup
	wg.Add(2)

	go d.diffOnChan(ch1, d.allKeys1, d.allKeys2, bar, &wg, size1)
	go d.diffOnChan(ch2, d.allKeys2, d.allKeys1, bar, &wg, size2)
	wg.Wait()
	return d.checkKeys("DB1", d.allKeys1) || d.checkKeys("DB2", d.allKeys2)
}

func (d *differ) checkKeys(name string, keys *sync.Map) bool {
	length := 0
	keys.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	if length > 0 {
		logger.Debugf("Remaining keys for %v: %+v", name, keys)
		return true
	}
	return false
}

func (d *differ) diffOnChan(ch chan []string, chanKeys, otherKeys *sync.Map,
	bar *pb.ProgressBar, wg *sync.WaitGroup, size int64) {
	defer wg.Done()
	if size == 0 {
		return
	}
	var matchesKey = regexp.MustCompile(d.ignoreKeys)
	for keys := range ch {
		for _, key := range keys {
			if d.ignoreKeys != "" && matchesKey.MatchString(key) {
				bar.Increment()
				continue
			}
			if _, ok := otherKeys.Load(key); ok {
				// Delete matching keys as we go and don't add them to avoid consuming too much memory.
				otherKeys.Delete(key)
			} else {
				chanKeys.Store(key, "")
			}
			bar.Increment()
		}
	}
}

func (d *differ) resultsDiffer(ch chan []string, size int64, bar *pb.ProgressBar) bool {
	if size == 0 {
		return false
	}
	processed := int64(0)
	for {
		keys := <-ch
		ktvs1, diffDetected1 := d.fetchKTVs(keys, sclient)
		ktvs2, diffDetected2 := d.fetchKTVs(keys, dclient)
		if diffDetected1 || diffDetected2 {
			logger.Debug("Diff detected")
			return true
		}

		if d.ktvArraysDiffer(ktvs1, ktvs2) {
			logger.Debug("KTV arrays differ")
			return true
		}

		processed += int64(len(keys))
		if processed == size {
			logger.Debug("Processed all keys")
			break
		}
	}
	return false
}

func (d *differ) ktvArraysDiffer(ktvs1, ktvs2 []*ktv) bool {
	size := len(ktvs1)

	for i := 0; i < size; i++ {
		a := ktvs1[i]
		b := ktvs2[i]
		if d.ktvsDiffer(a, b) {
			return true
		}
	}
	return false
}

func (d *differ) ktvsDiffer(a, b *ktv) bool {
	if a.key != b.key {
		logger.Debugf("Keys differ: %v, %v", a.key, b.key)
		return true
	}

	if a.ttl != b.ttl {
		logger.Debugf("TTLs differ for key %v: %v != %v", b.key, a.ttl, b.ttl)
		return true
	}

	if a.val != b.val {
		logger.Debugf("Vals differ for key %v: %v != %v", b.key, a.val, b.val)
		return true
	}
	return false
}

func (d *differ) fetchKTVs(keys []string, rclient *redis.Client) ([]*ktv, bool) {
	ktvs := make([]*ktv, 0)
	vals, err := rclient.MGet(keys...).Result()
	if err != nil {
		panic(fmt.Sprintf("Error reading keys %v", err))
	}

	for i, key := range keys {
		//logger.Debugf("val for index %v is %v", i, vals[i])
		if vals[i] == nil {
			// This indicates a non-string type.
			//logger.Debugf("val for index %v is %v for key %v", i, vals[i], key)
			// This indicates the key did not exist in the database.
			//return nil, true
			continue
		}
		ktv := &ktv{
			key: key,
			val: vals[i].(string),
			// This is what the redis client sets when there's no TTL.
			ttl: -1 * time.Millisecond,
		}
		ktvs = append(ktvs, ktv)
	}
	return ktvs, false
}
