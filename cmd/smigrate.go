package cmd

import (
	"sync"

	"github.com/spf13/cobra"
)

type smigrator struct {
	key string
}

var scount int

// smigrateCmd is for migrating a particular hash to a new redis.
var smigrateCmd = &cobra.Command{
	Use:   "smigrate",
	Short: "Migrate a large set at the specified key",
	Long: `Redis DUMP, RESTORE, and MIGRATE commands do not support key values larger than 512MB. This
uses HSCAN to migrate large sets. This is essentially akin to a theoretical SMIGRATE redis
command.`,
	Run: smigrate,
}

func init() {
	rootCmd.AddCommand(smigrateCmd)
	smigrateCmd.Flags().StringVarP(&cmdKey, "key", "k", "", "The key of the set to migrate")
	smigrateCmd.MarkFlagRequired("key")
	smigrateCmd.Flags().IntVarP(&scount, "scount", "", 5000, "The number of set entries to scan on each pass")
}

func smigrate(cmd *cobra.Command, args []string) {
	// This is a dummy waitgroup. The waitgroup is really only used when migrating large hashes as
	// a part of a larger migration.
	var wg sync.WaitGroup
	var sm = &smigrator{key: cmdKey}
	sm.migrate(&wg, dummyProgressPool)
}

func smigrateKey(k string, wg *sync.WaitGroup, pf poolFunc) int {
	var sm = &smigrator{key: k}
	return sm.migrate(wg, pf)
}

func (sm *smigrator) migrate(wg *sync.WaitGroup, pf poolFunc) int {
	return genericMigrateWith(sm.key, sm.sscan, sm.migrateKeyVals,
		sclient.SCard, wg, pf, scount)
}

func (sm *smigrator) sscan(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return sclient.SScan(key, cursor, match, count).Result()
}

func (sm *smigrator) migrateKeyVals(key string, keyvals []string) (addToTotal, error) {
	if len(keyvals) == 0 || dryRun {
		return identity, nil
	}
	cmd := dclient.SAdd(key, keyvals)
	_, err := cmd.Result()
	return identity, err
}
