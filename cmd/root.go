package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	pb "gopkg.in/cheggaaa/pb.v1"
)

var logger *zap.SugaredLogger

type migrator struct {
	cfgFile string

	src string
	dst string

	srcauth string
	dstauth string

	tlssrcCert string
	tlsdstCert string

	tlssrc bool
	tlsdst bool

	srcdb int
	dstdb int

	flushdst bool
	flushsrc bool

	tempHashes []string

	tempLists []string

	tempSets []string

	count int

	// Skip showing progress bars if true.
	noProgress bool
}

// Do not actually transfer the data.
var dryRun bool

var largeKeys = make(map[string]migFunc)

var mig = newMigrator()

// Source redis client.
var sclient *redis.Client

// Destination redis client.
var dclient *redis.Client

const hashKeys = "hashKeys"
const setKeys = "setKeys"
const listKeys = "listKeys"

// rootCmd migrates from one database to another using DUMP and RESTORE and including support for
// large hashes.
var rootCmd = &cobra.Command{
	Use:   "redistrict",
	Short: "CLI utility for migrating redis data from one database to another",
	Long: `A program for migrating redis databases particularly when you don't have SSH
access to the destination machine. This uses DUMP and RESTORE for all keys except when the caller
specifies key names of large hashes to migrate separately, as DUMP and RESTORE don't support hashes larger
than 512MBs. More details are at https://github.com/antirez/redis/issues/757

You can specify large hashes using the --hashKeys, --setKeys, or --listKeys flags or by
specifying similar in $HOME/.redistrict.yaml, as in:

hashKeys:
  - key->value
  - largeHash
  - evenLarger

setKeys:
  - key->value
  - largeHash
  - evenLarger

The command line flags override the config file. DOES NOT CURRENTLY SUPPORT SORTED SETS.`,
	Run: mig.migrate,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	dev, _ := zap.NewDevelopment()
	logger = dev.Sugar()
	cobra.OnInitialize(mig.initAll)

	rootCmd.PersistentFlags().StringSliceVar(&mig.tempHashes, hashKeys, make([]string, 0),
		"Key names of large hashes to automatically call hmigrate on, in the form --hashKeys=\"k1,k2\"")
	rootCmd.PersistentFlags().StringSliceVar(&mig.tempSets, setKeys, make([]string, 0),
		"Key names of large sets to automatically call smigrate on, in the form --setKeys=\"k1,k2\"")
	rootCmd.PersistentFlags().StringSliceVar(&mig.tempLists, listKeys, make([]string, 0),
		"Key names of large lists to automatically call lmigrate on, in the form --listKeys=\"k1,k2\"")
	rootCmd.PersistentFlags().StringVar(&mig.cfgFile, "config", "", "config file (default is $HOME/.redistrict.yaml)")
	rootCmd.PersistentFlags().StringVarP(&mig.src, "src", "s", "127.0.0.1:6379", "Source redis host IP/name")
	rootCmd.PersistentFlags().StringVarP(&mig.dst, "dst", "d", "127.0.0.1:6379", "Destination redis host IP/name")

	rootCmd.PersistentFlags().StringVarP(&mig.srcauth, "srcauth", "", "", "Source redis password")
	rootCmd.PersistentFlags().StringVarP(&mig.dstauth, "dstauth", "", "", "Destination redis password")
	rootCmd.PersistentFlags().StringVarP(&mig.tlssrcCert, "tlssrcCert", "", "", "TLS certificate path for source redis, if any. Implies tlssrc.")
	rootCmd.PersistentFlags().StringVarP(&mig.tlsdstCert, "tlsdstCert", "", "", "TLS certificate path for destination redis, if any. Implies tlsdst.")
	rootCmd.PersistentFlags().BoolVarP(&mig.tlssrc, "tlssrc", "", false, "Use TLS to access the source.")
	rootCmd.PersistentFlags().BoolVarP(&mig.tlsdst, "tlsdst", "", false, "Use TLS to access the destination.")
	rootCmd.PersistentFlags().IntVarP(&mig.srcdb, "srcdb", "", 0, "Redis db number, defaults to 0")
	rootCmd.PersistentFlags().IntVarP(&mig.dstdb, "dstdb", "", 0, "Redis db number, defaults to 0")
	rootCmd.PersistentFlags().BoolVarP(&mig.noProgress, "noprogress", "", false, "Do not display progress bars.")
	rootCmd.PersistentFlags().BoolVarP(&dryRun, "dryrun", "", false, "Do not actually perform the transfer.")

	rootCmd.PersistentFlags().BoolVarP(&mig.flushdst, "flushdst", "", false, "Flush the destination db before doing anything")
	rootCmd.Flags().IntVarP(&mig.count, "count", "", 5000, "The number of keys to scan on each pass")
}

// newMigrator returns a new empty migrator.
func newMigrator() *migrator {
	return &migrator{
		src:        "127.0.0.1:6379",
		dst:        "127.0.0.1:6379",
		tempHashes: make([]string, 0),
		tempLists:  make([]string, 0),
		tempSets:   make([]string, 0),
		noProgress: false,
	}
}

// initAll initializes any necessary services, such as config and redis.
func (m *migrator) initAll() {
	m.initConfig()
	m.initRedis()
}

func (m *migrator) writingToSelf() bool {
	return m.src == m.dst && m.srcdb == m.dstdb
}

// initRedis creates initial redis connections.
func (m *migrator) initRedis() {
	if m.flushdst {
		// Flush with a separate client because it can take ahwile on large DBs and can cause
		// timeouts to be hit.
		logger.Info("Flushing destination redis...")
		flushclient := m.newClient(m.dst, m.dstauth, m.dstdb, m.tlsdstCert, m.tlsdst)
		flushclient.FlushDB()
		flushclient.Close()
		logger.Info("Finished flushing destination redis...")
	}

	dclient = m.newClient(m.dst, m.dstauth, m.dstdb, m.tlsdstCert, m.tlsdst)
	sclient = m.newClient(m.src, m.srcauth, m.srcdb, m.tlssrcCert, m.tlssrc)

	// Note this is only exposed for tests to avoid letting the caller do something stupid...
	if m.flushsrc {
		//sclient.FlushDB()
	}
}

func (m *migrator) newClient(addr, password string, db int, certPath string, useTLS bool) *redis.Client {
	options := &redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		ReadTimeout:  10 * time.Minute,
		WriteTimeout: 10 * time.Minute,
		DialTimeout:  12 * time.Second,
	}
	if certPath != "" {
		options.TLSConfig = m.tlsConfig(certPath, addr)
	} else if useTLS {
		options.TLSConfig = &tls.Config{}
	}
	client := redis.NewClient(options)

	if err := client.Ping().Err(); err != nil {
		panic(fmt.Sprintf("Could not get pingable redis client: %v", err))
	}
	return client
}

func (m *migrator) tlsConfig(certPath, addr string) *tls.Config {

	cfg := &tls.Config{}

	if certPath != "" {
		caCert, err := ioutil.ReadFile(certPath)
		if err != nil {
			log.Fatal(err)
		}

		srcCertPool := x509.NewCertPool()
		srcCertPool.AppendCertsFromPEM(caCert)
		cfg.RootCAs = srcCertPool

		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			log.Fatalf("Unable to determine hostname for server: %v", err)
		}
		cfg.ServerName = host
	}

	return cfg
}

type scan func(cursor uint64, match string, count int64) *redis.ScanCmd

type klen func() *redis.IntCmd

func (m *migrator) migrate(cmd *cobra.Command, args []string) {
	m.integrateConfigSettings(m.tempHashes, hmigrateKey)
	m.integrateConfigSettings(m.tempSets, smigrateKey)
	m.integrateConfigSettings(m.tempLists, lmigrateKey)

	m.migrateKeys()
}

func (m *migrator) integrateConfigSettings(keys []string, mFunc migFunc) {
	for _, set := range keys {
		largeKeys[set] = mFunc
	}
}

type progress interface {
	Finish()
	Add(int) int
}

type poolFunc func(...*pb.ProgressBar) bool

type dummyProg struct{}

func (d *dummyProg) Finish()     {}
func (d *dummyProg) Add(int) int { return 0 }

var dummyProgressPool = func(...*pb.ProgressBar) bool { return false }

func (m *migrator) migrateKeys() {
	if m.writingToSelf() {
		fmt.Println("Source and destination databases cannot be the same. Consider using a different database ID.")
		return
	}
	length, err := sclient.DBSize().Result()
	if err != nil {
		panic(fmt.Sprintf("Error getting source database size: %v", err))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var bar progress
	var pf poolFunc
	if m.noProgress {
		bar = &dummyProg{}
		pf = dummyProgressPool
	} else {
		realProgress := pb.New(int(length)).Prefix("KEYS *")
		pool, err := pb.StartPool(realProgress)
		if err != nil {
			panic(err)
		}
		bar = realProgress
		pf = func(pbs ...*pb.ProgressBar) bool {
			pool.Add(pbs...)
			return true
		}

	}

	for k, migrateFunc := range largeKeys {
		go migrateFunc(k, &wg, pf)
	}

	ch := make(chan []string)

	go genericRead("", func(key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
		return sclient.Scan(cursor, "", count).Result()
	}, ch, m.count)
	m.write(ch, bar, &wg)
}

func (m *migrator) write(ch chan []string, bar progress, wg *sync.WaitGroup) {
	type ktv struct {
		key      string
		ttlCmd   *redis.DurationCmd
		valueCmd *redis.StringCmd
	}

	largeKeyCount := 0
	for keyvals := range ch {
		ktvs := make([]ktv, 0)
		spipeline := sclient.Pipeline()
		n := len(keyvals)
		for i := 0; i < n; i++ {
			key := keyvals[i]
			if _, ok := largeKeys[key]; ok {
				largeKeyCount++
				continue
			}
			ttlCmd := spipeline.PTTL(key)
			dumpCmd := spipeline.Dump(key)
			ktvs = append(ktvs, ktv{key: key, ttlCmd: ttlCmd, valueCmd: dumpCmd})
		}
		if _, err := spipeline.Exec(); err != nil {
			panic(fmt.Sprintf("Error execing source pipeline: %v", err))
		}

		dpipeline := dclient.Pipeline()
		for _, ktv := range ktvs {
			ttl, err := ktv.ttlCmd.Result()
			if err != nil {
				panic(fmt.Sprintf("Error reading key %v: %v", ktv.key, err))
			}
			// A TTL of less than 0 simply means there is no TTL. Specifying 0 when
			// we call RESTORE similarly sets no TTL.
			if ttl < 0 {
				ttl = 0
			} else if !strings.HasPrefix(ktv.key, "code") {
				logger.Debugf("Key %v has expiry set to %v", ktv.key, ttl)
			}
			value, err := ktv.valueCmd.Result()
			if err != nil {
				panic(fmt.Sprintf("Error reading value for key %v: %v", ktv.key, err))
			}
			dpipeline.Restore(ktv.key, ttl, value)
		}
		if !dryRun {
			if _, err := dpipeline.Exec(); err != nil {
				panic(fmt.Sprintf("Error execing destination pipeline: %v", err))
			}
		}
		bar.Add(n)
	}
	bar.Finish()
	wg.Done()
	//logger.Infof("Waiting on %v large hashes to complete transferring", largeKeyCount)
	wg.Wait()
}

// initConfig reads in config file and ENV variables if set.
func (m *migrator) initConfig() {
	if m.cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(m.cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".redistrict" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".redistrict")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		m.populateKeyMap(hashKeys, hmigrateKey)
		m.populateKeyMap(setKeys, smigrateKey)
		m.populateKeyMap(listKeys, lmigrateKey)
	}
}

type migFunc func(string, *sync.WaitGroup, poolFunc) int

func (m *migrator) populateKeyMap(keysName string, mFunc migFunc) {
	m.populateKeyMapFrom(keysName, viper.GetStringSlice, mFunc)
}

func (m *migrator) populateKeyMapFrom(keysName string, sliceFunc func(string) []string,
	mFunc migFunc) {
	keys := sliceFunc(keysName)
	for _, k := range keys {
		largeKeys[k] = mFunc
	}
}
