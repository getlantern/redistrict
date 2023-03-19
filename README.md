[![Go Status](https://github.com/getlantern/redistrict/actions/workflows/go.yml/badge.svg)](https://github.com/getlantern/redistrict/actions/workflows/go.yml)

## redistrict
CLI utility written in Go for migrating redis data. Redistrict is particularly useful if you need
to migrate large values (over 512MBs) that otherwise cannot be migrated with DUMP and RESTORE.
Redistrict migrates those "manually" using the relevant SCAN variants, such as HSCAN and SSCAN, along
with the related bulk write methods, such as HMSET and SADD. Uses pipelining where appropriate and
performs migrations of general keys and large keys in parallel.

NOTE THIS DOES NOT CURRENTLY SUPPORT MIGRATING LARGE SORTED SETS.

To find large keys in your database you can run:

```
./redis-cli --bigkeys
```

Redistrict also supports diffing two databases. This processes isn't quite as optimized as migration,
so it's a bit slow, but unlike other options available it will work with large keys along the same
lines as redistrict migration using the relevant scan variants.

### Install

```
go install github.com/getlantern/redistrict@latest
```

### Run

This will, for example, run redistrict with no progress bar, with TLS at the destination, and the source redis running locally. This will also **flush -- aka clear all data from -- the destination database on startup -- USE WITH CAUTION!**.

```
redistrict --noprogress --srcauth [source-password] --src 127.0.0.1:9736 --dstauth [destination-password] --dst [destination-url] --tlsdst --flushdst
```

### Synopsis

A program for migrating redis databases particularly when you don't have SSH
access to the destination machine. This uses DUMP and RESTORE for all keys except when the caller
specifies key names of large hashes to migrate separately, as DUMP and RESTORE don't support hashes larger
than 512MBs. More details are at https://github.com/antirez/redis/issues/757

You can specify large hashes using the --hashKeys, --setKeys, or --listKeys flags or by
specifying similar in $HOME/.redistrict.yaml, as in:

```
hashKeys:
  - key->value
  - largeHash
  - evenLarger

setKeys:
  - key->value
  - largeHash
  - evenLarger
```

The command line flags override the config file. DOES NOT CURRENTLY SUPPORT SORTED SETS.

```
redistrict [flags]
```

### Options

```
      --config string       config file (default is $HOME/.redistrict.yaml)
      --count int           The number of keys to scan on each pass (default 5000)
      --dryrun              Do not actually perform the transfer.
  -d, --dst string          Destination redis host IP/name (default "127.0.0.1:6379")
      --dstauth string      Destination redis password
      --dstdb int           Redis db number, defaults to 0
      --flushdst            Flush the destination db before doing anything
      --hashKeys strings    Key names of large hashes to automatically call hmigrate on, in the form --hashKeys="k1,k2"
  -h, --help                help for redistrict
      --listKeys strings    Key names of large lists to automatically call lmigrate on, in the form --listKeys="k1,k2"
      --noprogress          Do not display progress bars.
      --setKeys strings     Key names of large sets to automatically call smigrate on, in the form --setKeys="k1,k2"
  -s, --src string          Source redis host IP/name (default "127.0.0.1:6379")
      --srcauth string      Source redis password
      --srcdb int           Redis db number, defaults to 0
      --tlsdst              Use TLS to access the destination.
      --tlsdstCert string   TLS certificate path for destination redis, if any. Implies tlsdst.
      --tlssrc              Use TLS to access the source.
      --tlssrcCert string   TLS certificate path for source redis, if any. Implies tlssrc.
```

### SEE ALSO

* [redistrict diff](redistrict_diff.md)	 - Compares two redis databases
* [redistrict hmigrate](redistrict_hmigrate.md)	 - Migrate a large hash at the specified key
* [redistrict lmigrate](redistrict_lmigrate.md)	 - Migrate a large list at the specified key
* [redistrict smigrate](redistrict_smigrate.md)	 - Migrate a large set at the specified key
