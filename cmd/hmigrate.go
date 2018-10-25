package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// hmigrateCmd is for migrating a particular hash to a new redis.
var hmigrateCmd = &cobra.Command{
	Use:   "hmigrate",
	Short: "Migrate a large hash at the specified key",
	Long: `Redis DUMP, RESTORE, and MIGRATE commands do not support hashes larger than 512MB. This
uses HSCAN to migrate large hashes.`,
	Run: hmigrate,
}

var key string

func init() {
	rootCmd.AddCommand(hmigrateCmd)
	hmigrateCmd.Flags().StringVarP(&key, "key", "k", "", "The key of the hash to migrate")
	hmigrateCmd.MarkFlagRequired("key")
}

func hmigrate(cmd *cobra.Command, args []string) {
	fmt.Printf("hmigrate called with key %+v\n", key)
	var cursor uint64
	var n int
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = sclient.HScan(key, cursor, "", 100).Result()

		if err != nil {
			panic(err)
		}
		n += len(keyvals)

		fmt.Printf("Got %v keyvals", n)

		// This is a little quirky. Redis scans return keys followed by values, so to create a map
		// for a subsequent hmset call we have to iterate one forward in the array to map the value.
		hmap := make(map[string]interface{})
		for i, keyval := range keyvals {
			hmap[keyval] = keyvals[i+1]
			if n == i+2 {
				break
			}
		}
		dclient.HMSet(key, hmap)
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("found %d keys\n", n)
}
