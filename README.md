# redisync

A fast, concurrent Redis database synchronization tool written in Go.

## Features

- Synchronize all keys from one Redis instance to another
- Concurrent workers for high performance (default: 20 workers)
- Supports both `redis://` and `rediss://` (TLS) connections
- Preserves TTL values during sync
- Uses DUMP/RESTORE for accurate key replication

## Installation

```bash
go install github.com/andys/redisync@latest
```

Or build from source:

```bash
git clone https://github.com/andys/redisync.git
cd redisync
go build
```

## Usage

```bash
redisync -from <source-url> -to <destination-url> [-workers N]
```

### Options

- `-from` - Source Redis URL (required)
- `-to` - Destination Redis URL (required)
- `-workers` - Number of concurrent workers (default: 20)

### Examples

```bash
# Basic sync between two Redis instances
redisync -from redis://localhost:6379 -to redis://localhost:6380

# With authentication
redisync -from redis://user:password@source:6379 -to redis://user:password@dest:6379

# With TLS
redisync -from rediss://source:6379 -to rediss://dest:6379

# With more workers for faster sync
redisync -from redis://source:6379 -to redis://dest:6379 -workers 50
```

## How It Works

1. Scans all keys from the source Redis using SCAN
2. For each key, retrieves the TTL and serialized value using DUMP
3. Restores the key to the destination using RESTORE with REPLACE option
4. Preserves original TTL values

## License

MIT License - see [LICENSE](LICENSE) file for details.
