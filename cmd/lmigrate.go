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
	lm.migrate(nil, &wg)
}

func lmigrateKey(k string, bar *pb.ProgressBar, wg *sync.WaitGroup) {
	var lm = &lmigrator{key: k}
	lm.migrate(bar, wg)
}

func (lm *lmigrator) migrate(bar *pb.ProgressBar, wg *sync.WaitGroup) int {
	return genericMigrateWith(lm.key, lm.lscan, lm.migrateKeyVals,
		sclient.LLen, bar, wg)
}

func (lm *lmigrator) lscan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	var newCursor uint64
	newCursor = cursor + uint64(count)

	// Note these conversions from uint64 to int64 are generally ill-advised of course. In this case,
	// though, the cursor inherently starts at 0 and can never add more than count, which are both
	// int64s, and presumably the size of lists is limited to a int64, so we should be good.
	keyvals, err := sclient.LRange(key, int64(cursor), int64(newCursor)).Result()
	logger.Debugf("Got results...start %v, stop %v", int64(cursor), int64(newCursor))
	logger.Debugf("Error? %v length: %v", err, sclient.LLen(key))

	// The returned values having zero length indicates having iterated past the end of the list, so
	// return a cursor of 0, which is the signifier to break out of the loop for SCAN methods the
	// generic code uses.
	if len(keyvals) == 0 {
		return keyvals, 0, err
	}
	return keyvals, newCursor, err
}

func (lm *lmigrator) migrateKeyVals(key string, keyvals []string) resultable {
	return func() error {
		if len(keyvals) == 0 {
			return nil
		}
		cmd := dclient.LPush(key, keyvals)
		logger.Debug("Pushed...")
		_, err := cmd.Result()
		return err
	}
}
