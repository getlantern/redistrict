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
)

var cfgFile string

var src string
var dst string

var srcauth string
var dstauth string

var sslsrcCert string
var ssldstCert string

var db int

var flushdst bool

var sclient *redis.Client
var dclient *redis.Client

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "redistrict",
	Short: "Utility for migrating redis data from one database to another",
	Long: `A program for migrating redis databases particularly when you don't have SSH
access to the destination machine. This also solves edge cases such as hashes
that are too big for DUMP, RESTORE, and MIGRATE (bigger than 512MB).`,
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
	cobra.OnInitialize(initRedis)

	rootCmd.PersistentFlags().StringVarP(&src, "src", "s", "127.0.0.1:6379", "Source redis host IP/name")
	rootCmd.PersistentFlags().StringVarP(&dst, "dst", "d", "127.0.0.1:6379", "Destination redis host IP/name")

	rootCmd.PersistentFlags().StringVarP(&srcauth, "srcauth", "", "", "Source redis password")
	rootCmd.PersistentFlags().StringVarP(&dstauth, "dstauth", "", "", "Destination redis password")
	rootCmd.PersistentFlags().StringVarP(&sslsrcCert, "sslsrcCert", "", "", "SSL certificate path for source redis, if any.")
	rootCmd.PersistentFlags().StringVarP(&ssldstCert, "ssldstCert", "", "", "SSL certificate path for destination redis, if any.")
	rootCmd.PersistentFlags().IntVarP(&db, "db", "", 0, "Redis db number, defaults to 0")

	rootCmd.PersistentFlags().BoolVarP(&flushdst, "flushdst", "", false, "Flush the destination db before doing anything")
}

// initRedis creates initial redis connections.
func initRedis() {
	sclient = newClient(src, srcauth, db, sslsrcCert)
	dclient = newClient(dst, dstauth, db, ssldstCert)

	if flushdst {
		dclient.FlushDB()
	}
}

func newClient(addr, password string, db int, sslCert string) *redis.Client {
	options := &redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		ReadTimeout:  80 * time.Second,
		WriteTimeout: 80 * time.Second,
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
