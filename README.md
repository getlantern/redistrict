[![CircleCI](https://circleci.com/gh/getlantern/redistrict.svg?style=svg)](https://circleci.com/gh/getlantern/redistrict)

## redistrict
CLI utility written in Go for migrating redis data. You can install it with:

```
go get -u github.com/getlantern/redistrict
```

### Synopsis

A program for migrating redis databases particularly when you don't have SSH
access to the destination machine. This uses DUMP and RESTORE for all keys except when the caller
specifies key names of large hashes to migrate separately, as DUMP and RESTORE don't support hashes larger
than 512MBs. More details are at https://github.com/antirez/redis/issues/757

You can specify large hashes using the --hashKeys flag or by specifying large-hashes in $HOME/.redistrict.yaml, as in:

large-hashes:
  - key->value
  - largeHash
  - evenLarger

The command line flags override the config file.

```
redistrict [flags]
```

### Options

```
      --config string       config file (default is $HOME/.redistrict.yaml)
  -d, --dst string          Destination redis host IP/name (default "127.0.0.1:6379")
      --dstauth string      Destination redis password
      --dstdb int           Redis db number, defaults to 0
      --flushdst            Flush the destination db before doing anything
      --hashKeys strings    Key names of large hashes to automatically call hmigrate on, in the form --hashKeys="k1,k2"
  -h, --help                help for redistrict
  -s, --src string          Source redis host IP/name (default "127.0.0.1:6379")
      --srcauth string      Source redis password
      --srcdb int           Redis db number, defaults to 0
      --ssldstCert string   SSL certificate path for destination redis, if any.
      --sslsrcCert string   SSL certificate path for source redis, if any.
```

### SEE ALSO

* [redistrict hmigrate](redistrict_hmigrate.md)	 - Migrate a large hash at the specified key
