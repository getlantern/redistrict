package cmd

import (
	"fmt"
	"sync"

	"github.com/go-redis/redis"
	pb "gopkg.in/cheggaaa/pb.v1"
)

var cmdKey string

type gscan func(key string, cursor uint64, match string, count int64) ([]string, uint64, error)
type gmigrate func(key string, keyvals []string) (addToTotal, error)
type glen func(key string) *redis.IntCmd

type keyValHandler func(key string, scan gscan, gmig gmigrate, gl glen, bar *pb.ProgressBar,
	wg *sync.WaitGroup) int

func genericMigrateWith(key string, scan gscan, gmig gmigrate, gl glen,
	wg *sync.WaitGroup, pool *pb.Pool, count int) int {
	wg.Add(1)

	length, err := gl(key).Result()
	if err != nil {
		panic(fmt.Sprintf("Could not get length %v", err))
	}
	bar := pb.New(int(length)).Prefix(key)
	if pool != nil {
		pool.Add(bar)
	} else {
		bar.ShowTimeLeft = true
		bar.Start()
	}

	ch := make(chan []string)

	go genericRead(key, scan, ch, count)
	return genericWrite(key, gmig, ch, bar, wg)
}

// Generic read func that migrates things that can use variations on SCAN,
// such as hashes and sets (HSCAN and SSCAN).
func genericRead(key string, scan gscan, ch chan []string, count int) {
	var cursor uint64
	var n int64
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = scan(key, cursor, "", int64(count))
		if err != nil {
			panic(fmt.Sprintf("Error scanning source: %v", err))
		}
		cur := len(keyvals)
		n += int64(cur)

		if cur > 0 {
			ch <- keyvals
		}

		if cursor == 0 {
			close(ch)
			break
		}
	}
}

// addToTotal is simply a function that returns the correct number to add to the total for a
// given pass. This is purely because hscan returns the keys and values in the response, so when
// adding to the total keys processed we need to use half the length.
type addToTotal func(i int) int

var identity = func(i int) int { return i }

var half = func(i int) int { return i / 2 }

func genericWrite(key string, gmig gmigrate, ch chan []string, bar *pb.ProgressBar, wg *sync.WaitGroup) int {
	defer wg.Done()
	total := 0
	for keyvals := range ch {
		adder, err := gmig(key, keyvals)
		if err != nil {
			panic(fmt.Sprintf("Error setting values on destination for key %v: %v", key, err))
		} else {
			cur := len(keyvals)
			toAdd := adder(cur)
			bar.Add(toAdd)
			total += toAdd
		}
	}
	bar.Finish()
	return total
}
