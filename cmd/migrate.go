package cmd

import (
	"fmt"
	"sync"

	"github.com/go-redis/redis"
	pb "gopkg.in/cheggaaa/pb.v1"
)

var cmdKey string

type gscan func(key string, cursor uint64, match string, count int64) ([]string, uint64, error)
type gset func(key string, keyvals []string) resultable
type glen func(key string) *redis.IntCmd

type resultable func() error

type keyValHandler func(key string, scan gscan, set gset, gl glen, bar *pb.ProgressBar,
	wg *sync.WaitGroup) int

func genericMigrateWith(key string, scan gscan, set gset, gl glen,
	wg *sync.WaitGroup, pool *pb.Pool) int {
	wg.Add(1)

	length, err := gl(key).Result()
	if err != nil {
		panic(fmt.Sprintf("Could not get length %v", err))
	}
	bar := pb.New(int(length)).Prefix(key)
	if pool != nil {
		pool.Add(bar)
	}

	ch := make(chan []string)

	go genericRead(key, scan, ch)
	return genericWrite(key, set, ch, bar, wg)
}

// Generic read func that migrates things that can use variations on SCAN,
// such as hashes and sets (HSCAN and SSCAN).
func genericRead(key string, scan gscan, ch chan []string) {
	var cursor uint64
	var n int64
	//for i := 0; i < 10; i++ {
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = scan(key, cursor, "", int64(hcount))
		if err != nil {
			panic(err)
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

func genericWrite(key string, set gset, ch chan []string, bar *pb.ProgressBar, wg *sync.WaitGroup) int {
	defer wg.Done()
	total := 0
	for keyvals := range ch {
		err := set(key, keyvals)()
		if err != nil {
			panic(fmt.Sprintf("Error setting values on destination %v", err))
		} else {
			cur := len(keyvals)
			bar.Add(cur)
			total += cur
		}
	}
	bar.Finish()
	return total
}
