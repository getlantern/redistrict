package cmd

import (
	"sync"

	"github.com/spf13/cobra"
	pb "gopkg.in/cheggaaa/pb.v1"
)

type hmigrator struct {
	key string
}

var hcount int

// hmigrateCmd is for migrating a particular hash to a new redis.
var hmigrateCmd = &cobra.Command{
	Use:   "hmigrate",
	Short: "Migrate a large hash at the specified key",
	Long: `Redis DUMP, RESTORE, and MIGRATE commands do not support hashes larger than 512MB. This
uses HSCAN to migrate large hashes. This is essentially akin to a theoretical HMIGRATE redis
command.`,
	Run: hmigrate,
}

func init() {
	rootCmd.AddCommand(hmigrateCmd)
	hmigrateCmd.Flags().StringVarP(&cmdKey, "key", "k", "", "The key of the hash to migrate")
	hmigrateCmd.MarkFlagRequired("key")
	hmigrateCmd.Flags().IntVarP(&hcount, "hcount", "", 5000, "The number of hash entries to scan on each pass")
}

func hmigrate(cmd *cobra.Command, args []string) {
	// This is a dummy waitgroup. The waitgroup is really only used when migrating large hashes as
	// a part of a larger migration.
	var wg sync.WaitGroup
	var hm = &hmigrator{key: cmdKey}
	hm.migrate(&wg, nil)
}

func hmigrateKey(k string, wg *sync.WaitGroup, pool *pb.Pool) int {
	var hm = &hmigrator{key: k}
	return hm.migrate(wg, pool)
}

func (hm *hmigrator) migrate(wg *sync.WaitGroup, pool *pb.Pool) int {
	return genericMigrateWith(hm.key, hm.hscan, hm.migrateKeyVals,
		sclient.HLen, wg, pool)
}

func (hm *hmigrator) hscan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return sclient.HScan(key, cursor, match, count).Result()
}

func (hm *hmigrator) migrateKeyVals(key string, keyvals []string) (addToTotal, error) {
	if len(keyvals) == 0 {
		return half, nil
	}
	cmd := dclient.HMSet(key, hm.keyvalsToMap(keyvals))
	_, err := cmd.Result()
	return half, err
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
