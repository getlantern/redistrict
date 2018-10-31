package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

type migrator struct {
	cfgFile string

	src string
	dst string

	srcauth string
	dstauth string

	sslsrcCert string
	ssldstCert string

	srcdb int
	dstdb int

	flushdst bool
	flushsrc bool

	largeHashes map[string]bool

	tempHashes []string

	count int
}

var mig = &migrator{
	src:         "127.0.0.1:6379",
	dst:         "127.0.0.1:6379",
	largeHashes: make(map[string]bool),
	tempHashes:  make([]string, 0),
}

// Source redis client.
var sclient *redis.Client

// Destination redis client.
var dclient *redis.Client

// rootCmd migrates from one database to another using DUMP and RESTORE and including support for
// large hashes.
var rootCmd = &cobra.Command{
	Use:   "redistrict",
	Short: "CLI utility for migrating redis data from one database to another",
	Long: `A program for migrating redis databases particularly when you don't have SSH
access to the destination machine. This uses DUMP and RESTORE for all keys except when the caller
specifies key names of large hashes to migrate separately, as DUMP and RESTORE don't support hashes larger
than 512MBs. More details are at https://github.com/antirez/redis/issues/757

You can specify large hashes using the --hashKeys flag or by specifying large-hashes in $HOME/.redistrict.yaml, as in:

large-hashes:
  - key->value
  - largeHash
  - evenLarger

The command line flags override the config file.`,
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

	rootCmd.PersistentFlags().StringSliceVar(&mig.tempHashes, "hashKeys", make([]string, 0),
		"Key names of large hashes to automatically call hmigrate on, in the form --hashKeys=\"k1,k2\"")
	rootCmd.PersistentFlags().StringVar(&mig.cfgFile, "config", "", "config file (default is $HOME/.redistrict.yaml)")
	rootCmd.PersistentFlags().StringVarP(&mig.src, "src", "s", "127.0.0.1:6379", "Source redis host IP/name")
	rootCmd.PersistentFlags().StringVarP(&mig.dst, "dst", "d", "127.0.0.1:6379", "Destination redis host IP/name")

	rootCmd.PersistentFlags().StringVarP(&mig.srcauth, "srcauth", "", "", "Source redis password")
	rootCmd.PersistentFlags().StringVarP(&mig.dstauth, "dstauth", "", "", "Destination redis password")
	rootCmd.PersistentFlags().StringVarP(&mig.sslsrcCert, "sslsrcCert", "", "", "SSL certificate path for source redis, if any.")
	rootCmd.PersistentFlags().StringVarP(&mig.ssldstCert, "ssldstCert", "", "", "SSL certificate path for destination redis, if any.")
	rootCmd.PersistentFlags().IntVarP(&mig.srcdb, "srcdb", "", 0, "Redis db number, defaults to 0")
	rootCmd.PersistentFlags().IntVarP(&mig.dstdb, "dstdb", "", 0, "Redis db number, defaults to 0")

	rootCmd.PersistentFlags().BoolVarP(&mig.flushdst, "flushdst", "", false, "Flush the destination db before doing anything")
	rootCmd.Flags().IntVarP(&mig.count, "count", "", 5000, "The number of keys to scan on each pass")
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
	dclient = m.newClient(m.dst, m.dstauth, m.dstdb, m.ssldstCert)

	if m.flushdst {
		dclient.FlushDB()
	}

	sclient = m.newClient(m.src, m.srcauth, m.srcdb, m.sslsrcCert)

	// Note this is only exposed for tests to avoid letting the caller do something stupid...
	if m.flushsrc {
		//sclient.FlushDB()
	}
}

func (m *migrator) newClient(addr, password string, db int, sslCert string) *redis.Client {
	options := &redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		ReadTimeout:  10 * time.Minute,
		WriteTimeout: 10 * time.Minute,
		DialTimeout:  12 * time.Second,
	}
	if sslCert != "" {
		options.TLSConfig = m.tlsConfig(sslCert)
	}
	return redis.NewClient(options)
}

func (m *migrator) tlsConfig(certPath string) *tls.Config {
	cert, err := ioutil.ReadFile(certPath)
	if err != nil {
		log.Fatal(err)
	}

	srcCertPool := x509.NewCertPool()
	srcCertPool.AppendCertsFromPEM(cert)

	return &tls.Config{
		RootCAs: srcCertPool,
	}
}

type scan func(cursor uint64, match string, count int64) *redis.ScanCmd

type klen func() *redis.IntCmd

func (m *migrator) migrate(cmd *cobra.Command, args []string) {
	if len(m.tempHashes) > 0 {
		// Just make sure the command line fully overrides the config file.
		m.largeHashes = make(map[string]bool)
		for _, hash := range m.tempHashes {
			m.largeHashes[hash] = true
		}
	}

	m.migrateWith(sclient.Scan, sclient.DBSize)
}

func (m *migrator) migrateWith(sc scan, kl klen) {
	if m.writingToSelf() {
		fmt.Println("Source and destination databases cannot be the same. Consider using a different database ID.")
		return
	}
	logger.Infof("Fetching length from client %v", sclient)
	length, err := kl().Result()
	logger.Infof("Fetched length from client %v", sclient)
	if err != nil {
		panic(fmt.Sprintf("Error getting source database size: %v", err))
	}

	var wg sync.WaitGroup
	multi := mpb.New(mpb.WithWaitGroup(&wg))
	wg.Add(1 + len(m.largeHashes))

	bar := m.newBar(multi, length, "keys")

	// Just include the length of the large hashes in the total length.
	for k := range m.largeHashes {
		hl, err := sclient.HLen(k).Result()
		if err != nil {
			panic(fmt.Sprintf("Could not get hash length for %v:\n %v", k, err))
		}

		hbar := m.newBar(multi, hl, k)

		go hmigrateKey(k, hbar, &wg)
	}
	//bar := pb.StartNew(int(length))

	ch := make(chan []string)

	go m.read(sc, ch)
	m.write(ch, bar, &wg)
}

func (m *migrator) newBar(multi *mpb.Progress, length int64, name string) *mpb.Bar {
	return multi.AddBar(length,
		mpb.PrependDecorators(
			// simple name decorator
			decor.Name(name),
			// decor.DSyncWidth bit enables column width synchronization
			decor.Percentage(decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			// replace ETA decorator with "done" message, OnComplete event
			decor.OnComplete(
				// ETA decorator with ewma age of 60
				decor.EwmaETA(decor.ET_STYLE_GO, 60), "done",
			),
		),
	)
}

func (m *migrator) read(sc scan, ch chan []string) {
	var cursor uint64
	var n int64
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = sc(cursor, "", int64(m.count)).Result()
		if err != nil {
			panic(fmt.Sprintf("Error scanning source: %v", err))
		}
		cur := len(keyvals)
		n += int64(cur)

		ch <- keyvals

		if cursor == 0 {
			close(ch)
			break
		}
	}
}

func (m *migrator) write(ch chan []string, bar *mpb.Bar, wg *sync.WaitGroup) {
	type ktv struct {
		key      string
		ttlCmd   *redis.DurationCmd
		valueCmd *redis.StringCmd
	}

	largeKeyCount := 0
	for keyvals := range ch {
		ktvs := make([]ktv, 0)
		spipeline := sclient.Pipeline()
		//logger.Infof("Reading %v keys", len(keyvals))
		n := len(keyvals)
		for i := 0; i < n; i++ {
			key := keyvals[i]
			if _, ok := m.largeHashes[key]; ok {
				logger.Infof("Separately migrating large hash at %v", key)
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
			}
			value, err := ktv.valueCmd.Result()
			if err != nil {
				panic(fmt.Sprintf("Error reading value for key %v: %v", ktv.key, err))
			}
			dpipeline.Restore(ktv.key, ttl, value)
		}

		if _, err := dpipeline.Exec(); err != nil {
			panic(fmt.Sprintf("Error execing destination pipeline: %v", err))
		}
		bar.IncrBy(n)
		//bar.Add(n)
	}
	wg.Done()
	logger.Infof("Waiting on %v large hashes to complete transferring", largeKeyCount)
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
		hashes := viper.GetStringSlice("large-hashes")
		for _, hash := range hashes {
			m.largeHashes[hash] = true
		}
	}
}
