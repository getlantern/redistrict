package cmd

import (
	"fmt"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/go-redis/redis"
	"github.com/spf13/cobra"
	pb "gopkg.in/cheggaaa/pb.v1"
)

const compareCount = 4000

type differ struct {
	allKeys1      *sync.Map
	allKeys2      *sync.Map
	ignoreKeys    string
	valsProcessed uint64
}

var di = newDiffer()

// diffCmd is for comparing two redis databases.
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Compares two redis databases",
	Long:  `Compares two redis databases using scans and pipelining`,
	Run:   di.diffCommand,
}

func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.Flags().StringVarP(&di.ignoreKeys, "ignorekeys", "", "", "Regex for keys to ignore")
}

func newDiffer() *differ {
	return &differ{
		allKeys1: new(sync.Map),
		allKeys2: new(sync.Map),
	}
}

func (d *differ) diffCommand(cmd *cobra.Command, args []string) {
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

	var msg string
	if keysOnly || differ {
		msg = "The redises are different!"
	} else {
		msg = fmt.Sprintf("The redises are the same for all keys and for %v string values", d.valsProcessed)
	}
	bar.FinishPrint(msg)
	return differ
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
				atomic.AddUint64(&d.valsProcessed, 1)
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
		if d.valuesDiffer(keys, bar) {
			return true
		}

		processed += int64(len(keys))
		if processed == size {
			break
		}
	}
	return false
}

func (d *differ) valuesDiffer(keys []string, bar *pb.ProgressBar) bool {
	spl := sclient.Pipeline()
	for _, key := range keys {
		spl.Type(key)
	}

	cmds, err := spl.Exec()
	if err != nil {
		panic(fmt.Sprintf("Could not exec types pipeline: %v", err))
	}
	spl.Close()

	//dpl := dclient.Pipeline()
	for i, cmd := range cmds {
		keyType := d.toVal(cmd)
		switch keyType {
		case "string":
			if d.stringDiffers(keys[i]) {
				return true
			}
		case "list":
			if d.listDiffers(keys[i]) {
				return true
			}
		case "set":
			if d.setDiffers(keys[i], sclient.SScan, dclient.SScan, false) {
				return true
			}
		case "zset":
			if d.setDiffers(keys[i], sclient.ZScan, dclient.ZScan, true) {
				return true
			}
		case "hash":
			if d.hashDiffers(keys[i]) {
				return true
			}
		case "stream":
			logger.Debug("handling stream")
		default:
			logger.Debug("handling default")
		}
		atomic.AddUint64(&d.valsProcessed, uint64(1))
		bar.Increment()
	}
	return false
}

type resulter interface {
	Result() (string, error)
}

type sliceResulter interface {
	Result() ([]string, error)
}

type scanResulter interface {
	Result() ([]string, uint64, error)
}

func (d *differ) toVal(cmd redis.Cmder) string {
	switch cmd.(type) {
	case *redis.StatusCmd:
		return d.result(cmd.(*redis.StatusCmd))
	case *redis.StringCmd:
		return d.result(cmd.(*redis.StringCmd))
	default:
		logger.Debug("Got other type: %v *************", reflect.TypeOf(cmd))
	}
	panic(fmt.Sprintf("Could not handle command %v", cmd))
}

func (d *differ) result(res resulter) string {
	if val, err := res.Result(); err != nil {
		panic(fmt.Sprintf("Could not result for type pipeline: %v", err))
	} else {
		return val
	}
}

func (d *differ) sliceResult(res sliceResulter) []string {
	if val, err := res.Result(); err != nil {
		panic(fmt.Sprintf("Could not result for type pipeline: %v", err))
	} else {
		return val
	}
}

func (d *differ) scanResult(res scanResulter) ([]string, uint64) {
	if val, cursor, err := res.Result(); err != nil {
		panic(fmt.Sprintf("Could not result for type pipeline: %v", err))
	} else {
		return val, cursor
	}
}

func (d *differ) stringDiffers(key string) bool {
	return sclient.Get(key).Val() != dclient.Get(key).Val()
}

func (d *differ) listDiffers(key string) bool {
	const count = int64(10000)
	cursor := int64(0)
	for {
		newCursor := cursor + count
		svals := d.sliceResult(sclient.LRange(key, cursor, newCursor))
		dvals := d.sliceResult(dclient.LRange(key, cursor, newCursor))
		if len(svals) != len(dvals) {
			return true
		}
		for i, v := range svals {
			if v != dvals[i] {
				return true
			}
		}
		if len(svals) == 0 || len(dvals) == 0 {
			break
		}
		cursor = newCursor + 1
	}
	return false
}

func (d *differ) setDiffers(key string, source, dest scanf, skip bool) bool {
	diff, _ := d.setDiffersWithCount(key, int64(10000), source, dest, skip)
	return diff
}

type scanf func(string, uint64, string, int64) *redis.ScanCmd

func (d *differ) setDiffersWithCount(key string, count int64, source, dest scanf, skip bool) (bool, int) {
	smap := make(map[string]bool)
	dmap := make(map[string]bool)
	scursor := uint64(0)
	dcursor := uint64(0)
	processed := 0
	f := func(vals []string, a, b map[string]bool) {
		for i, v := range vals {
			// zsets include their score as well, which we skip.
			if skip && i%2 != 0 {
				continue
			}
			if _, ok := b[v]; ok {
				delete(b, v)
			} else {
				a[v] = true
			}
			processed++
		}
	}
	for {
		var svals, dvals []string
		svals, scursor = d.scanResult(source(key, scursor, "", count))
		dvals, dcursor = d.scanResult(dest(key, dcursor, "", count))
		f(svals, smap, dmap)
		f(dvals, dmap, smap)

		if scursor == 0 || dcursor == 0 {
			break
		}
	}
	if len(smap) != 0 || len(dmap) != 0 {
		return true, processed / 2
	}
	return false, processed / 2
}

func (d *differ) hashDiffers(key string) bool {
	differs, _ := d.hashDiffersWithCount(key, int64(10000))
	return differs
}

func (d *differ) hashDiffersWithCount(key string, count int64) (bool, int) {
	smap := make(map[string]string)
	dmap := make(map[string]string)

	scursor := uint64(0)
	dcursor := uint64(0)
	processed := 0
	f := func(kvs []string, a, b map[string]string) bool {
		for i := 0; i < len(kvs); i += 2 {
			k := kvs[i]
			v := kvs[i+1]
			if bv, ok := b[k]; ok {
				if bv == v {
					// Remove this key from the other DB map to save memory -- at this
					// point the keys and values check out and are equal.
					delete(b, k)
					processed++
				} else {
					return true
				}
			} else {
				a[k] = v
			}
		}
		return false
	}
	for {
		var svals, dvals []string
		svals, scursor = d.scanResult(sclient.HScan(key, scursor, "", count))
		dvals, dcursor = d.scanResult(dclient.HScan(key, dcursor, "", count))

		if f(svals, smap, dmap) {
			return true, processed
		}
		if f(dvals, dmap, smap) {
			return true, processed
		}

		if scursor == 0 || dcursor == 0 {
			break
		}
	}
	if len(smap) != 0 || len(dmap) != 0 {
		return true, processed
	}
	return false, processed
}
