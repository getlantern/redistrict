package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	pb "gopkg.in/cheggaaa/pb.v2"
)

var logger *zap.SugaredLogger

type migrator struct {
}

var cfgFile string

var src = "127.0.0.1:6379"
var dst = "127.0.0.1:6379"

var srcauth string
var dstauth string

var sslsrcCert string
var ssldstCert string

var srcdb int
var dstdb int

var flushdst bool
var flushsrc bool

var sclient *redis.Client
var dclient *redis.Client

var largeHashes = make(map[string]bool)

var tempHashes = make([]string, 0)

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
	Run: migrate,
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
	cobra.OnInitialize(initAll)

	rootCmd.PersistentFlags().StringSliceVar(&tempHashes, "hashKeys", make([]string, 0),
		"Key names of large hashes to automatically call hmigrate on, in the form --hashKeys=\"k1,k2\"")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.redistrict.yaml)")
	rootCmd.PersistentFlags().StringVarP(&src, "src", "s", "127.0.0.1:6379", "Source redis host IP/name")
	rootCmd.PersistentFlags().StringVarP(&dst, "dst", "d", "127.0.0.1:6379", "Destination redis host IP/name")

	rootCmd.PersistentFlags().StringVarP(&srcauth, "srcauth", "", "", "Source redis password")
	rootCmd.PersistentFlags().StringVarP(&dstauth, "dstauth", "", "", "Destination redis password")
	rootCmd.PersistentFlags().StringVarP(&sslsrcCert, "sslsrcCert", "", "", "SSL certificate path for source redis, if any.")
	rootCmd.PersistentFlags().StringVarP(&ssldstCert, "ssldstCert", "", "", "SSL certificate path for destination redis, if any.")
	rootCmd.PersistentFlags().IntVarP(&srcdb, "srcdb", "", 0, "Redis db number, defaults to 0")
	rootCmd.PersistentFlags().IntVarP(&dstdb, "dstdb", "", 0, "Redis db number, defaults to 0")

	rootCmd.PersistentFlags().BoolVarP(&flushdst, "flushdst", "", false, "Flush the destination db before doing anything")
}

// initAll initializes any necessary services, such as config and redis.
func initAll() {
	initConfig()
	initRedis()
}

// initRedis creates initial redis connections.
func initRedis() {
	sclient = newClient(src, srcauth, srcdb, sslsrcCert)
	dclient = newClient(dst, dstauth, dstdb, ssldstCert)

	if flushdst {
		dclient.FlushDB()
	}

	// Note this is only exposed for tests to avoid letting the caller do something stupid...
	if flushsrc {
		//sclient.FlushDB()
	}
}

func newClient(addr, password string, db int, sslCert string) *redis.Client {
	options := &redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		ReadTimeout:  20 * time.Minute,
		WriteTimeout: 20 * time.Minute,
		DialTimeout:  12 * time.Second,
	}
	if sslCert != "" {
		options.TLSConfig = tlsConfig(sslCert)
	}
	return redis.NewClient(options)
}

func tlsConfig(certPath string) *tls.Config {
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

func migrate(cmd *cobra.Command, args []string) {
	if len(tempHashes) > 0 {
		// Just make sure the command line fully overrides the config file.
		largeHashes = make(map[string]bool)
		for _, hash := range tempHashes {
			largeHashes[hash] = true
		}
	}

	m := &migrator{}
	m.migrateWith(sclient.Scan, sclient.DBSize)
}

func (m *migrator) migrateWith(sc scan, kl klen) {
	length, err := kl().Result()
	if err != nil {
		panic(err)
	}
	logger.Debugf("Migrating database with %v keys", length)
	bar := pb.StartNew(int(length))

	ch := make(chan []string)

	go m.read(sc, ch)
	m.write(ch, bar)
}

func (m *migrator) read(sc scan, ch chan []string) {
	var cursor uint64
	var n int64
	for {
		var keyvals []string
		var err error
		keyvals, cursor, err = sc(cursor, "", 10000).Result()
		if err != nil {
			panic(err)
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

func (m *migrator) write(ch chan []string, bar *pb.ProgressBar) {
	type ktv struct {
		key      string
		ttlCmd   *redis.DurationCmd
		valueCmd *redis.StringCmd
	}

	for keyvals := range ch {
		ktvs := make([]ktv, 0)
		spipeline := sclient.Pipeline()
		//logger.Infof("Reading %v keys", len(keyvals))
		n := len(keyvals)
		for i := 0; i < n; i++ {
			key := keyvals[i]
			if _, ok := largeHashes[key]; ok {
				logger.Infof("Separately migrating large hash at %v", key)
				hmigrateKey(key)
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
		bar.Add(n)
	}

	bar.Finish()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
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
			largeHashes[hash] = true
		}
	}
}
