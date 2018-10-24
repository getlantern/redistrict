# redistrict
CLI utility written in Go for migrating redis data. You can install it with:

```
go install github.com/getlantern/redistrict
```

Then you can view the command line options as follows:

$ ./redistrict
A program for migrating redis databases particularly when you don't have SSH
access to the destination machine. This also solves edge cases such as hashes
that are too big for DUMP, RESTORE, and MIGRATE (bigger than 512MB).

Usage:
  redistrict [command]

Available Commands:
  help        Help about any command
  hmigrate    Migrate a large hash at the specified key

Flags:
      --db int           Redis db number, defaults to 0
  -d, --dst string       Destination redis host IP/name (default "127.0.0.1:6379")
      --dstauth string   Destination redis password
      --flushdst         Flush the destination db before doing anything
  -h, --help             help for redistrict
  -s, --src string       Source redis host IP/name (default "127.0.0.1:6379")
      --srcauth string   Source redis password
      --ssldst           Set TLS/SSL flag for the destination redis
      --sslsrc           Set TLS/SSL flag for the source redis

Use "redistrict [command] --help" for more information about a command.
