package cmd

import (
	"sync"

	"github.com/spf13/cobra"
	pb "gopkg.in/cheggaaa/pb.v1"
)

type lmigrator struct {
	key string
}

var lcount int

// lmigrateCmd is for migrating a particular list to a new redis.
var lmigrateCmd = &cobra.Command{
	Use:   "lmigrate",
	Short: "Migrate a large list at the specified key",
	Long: `Redis DUMP, RESTORE, and MIGRATE commands do not support key values larger than 512MB. This
uses LRANGE to migrate large lists. This is essentially akin to a theoretical LMIGRATE redis
command.`,
	Run: lmigrate,
}

func init() {
	rootCmd.AddCommand(lmigrateCmd)
	lmigrateCmd.Flags().StringVarP(&cmdKey, "key", "k", "", "The key of the list to migrate")
	lmigrateCmd.MarkFlagRequired("key")
	lmigrateCmd.Flags().IntVarP(&lcount, "lcount", "", 5000, "The number of list entries to scan on each pass")
}

func lmigrate(cmd *cobra.Command, args []string) {
	// This is a dummy waitgroup. The waitgroup is really only used when migrating large hashes as
	// a part of a larger migration.
	var wg sync.WaitGroup
	var lm = &lmigrator{key: cmdKey}
	lm.migrate(&wg, nil)
}

func lmigrateKey(k string, wg *sync.WaitGroup, pool *pb.Pool) int {
	var lm = &lmigrator{key: k}
	return lm.migrate(wg, pool)
}

func (lm *lmigrator) migrate(wg *sync.WaitGroup, pool *pb.Pool) int {
	return genericMigrateWith(lm.key, lm.lscan, lm.migrateKeyVals,
		sclient.LLen, wg, pool)
}

func (lm *lmigrator) lscan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	var newCursor uint64
	newCursor = cursor + uint64(count)

	// Note these conversions from uint64 to int64 are generally ill-advised of course. In this case,
	// though, the cursor inherently starts at 0 and can never add more than count, which are both
	// int64s, and presumably the size of lists is limited to a int64, so we should be good.
	keyvals, err := sclient.LRange(key, int64(cursor), int64(newCursor)).Result()

	// The returned values having zero length indicates having iterated past the end of the list, so
	// return a cursor of 0, which is the signifier to break out of the loop for SCAN methods the
	// generic code uses.
	if len(keyvals) == 0 {
		return keyvals, 0, err
	}
	return keyvals, newCursor, err
}

func (lm *lmigrator) migrateKeyVals(key string, keyvals []string) (addToTotal, error) {
	if len(keyvals) == 0 {
		return identity, nil
	}
	cmd := dclient.LPush(key, keyvals)
	_, err := cmd.Result()
	return identity, err
}
