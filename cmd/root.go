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
	"github.com/spf13/cobra"
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

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "redistrict",
	Short: "Utility for migrating redis data from one database to another",
	Long: `A program for migrating redis databases particularly when you don't have SSH
access to the destination machine. This also solves edge cases such as hashes
that are too big for DUMP, RESTORE, and MIGRATE (bigger than 512MB).`,
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
	cobra.OnInitialize(initRedis)

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
		ReadTimeout:  80 * time.Second,
		WriteTimeout: 80 * time.Second,
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
	m := &migrator{}
	m.migrateWith(sclient.Scan, sclient.DBSize)
}

func (m *migrator) migrateWith(sc scan, kl klen) {
	length, err := kl().Result()
	if err != nil {
		panic(err)
	}
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
		keyvals, cursor, err = sc(cursor, "", 1000).Result()
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
		logger.Infof("Reading %v keys", len(keyvals))
		for i := 0; i < len(keyvals); i++ {
			key := keyvals[i]
			ttlCmd := spipeline.PTTL(key)
			dumpCmd := spipeline.Dump(key)
			ktvs = append(ktvs, ktv{key: key, ttlCmd: ttlCmd, valueCmd: dumpCmd})
		}
		spipeline.Exec()
		logger.Info("Ran exec on source pipeline...")

		dpipeline := dclient.Pipeline()
		for _, ktv := range ktvs {
			ttl, err := ktv.ttlCmd.Result()
			if err != nil {
				panic(err)
			}
			if ttl < 0 {
				//logger.Errorf("TTL is < 0 for key %v", ktv.key)
				//panic("TTL is " + ttl.String())
				ttl = 0
			}
			value, err := ktv.valueCmd.Result()
			if err != nil {
				panic(err)
			}
			dpipeline.Restore(ktv.key, ttl, value)
		}

		_, err := dpipeline.Exec()
		if err != nil {
			panic(err)
		}
		logger.Info("Ran exec on dest pipeline...")
	}

	bar.Finish()
}
