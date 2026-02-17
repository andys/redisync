package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/redis/go-redis/v9"
)

// useTypeBasedSync is set to true when Redis versions are incompatible for DUMP/RESTORE
var useTypeBasedSync bool

func main() {
	from := flag.String("from", "", "Source Redis URL (redis:// or rediss://)")
	to := flag.String("to", "", "Destination Redis URL (redis:// or rediss://)")
	workers := flag.Int("workers", 20, "Number of concurrent workers")
	forceTypeBased := flag.Bool("type-based", false, "Force type-based sync instead of DUMP/RESTORE")
	flag.Parse()

	if *from == "" || *to == "" {
		fmt.Fprintln(os.Stderr, "Usage: redisync -from <url> -to <url>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()

	srcClient, err := parseRedisURL(*from)
	if err != nil {
		log.Fatalf("Failed to parse source URL: %v", err)
	}
	defer srcClient.Close()

	dstClient, err := parseRedisURL(*to)
	if err != nil {
		log.Fatalf("Failed to parse destination URL: %v", err)
	}
	defer dstClient.Close()

	// Test connections and check version compatibility
	if err := srcClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to source Redis: %v", err)
	}
	if err := dstClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to destination Redis: %v", err)
	}

	// Check RDB version compatibility
	srcVersion, err := getRedisVersion(ctx, srcClient)
	if err != nil {
		log.Fatalf("Failed to get source Redis version: %v", err)
	}
	dstVersion, err := getRedisVersion(ctx, dstClient)
	if err != nil {
		log.Fatalf("Failed to get destination Redis version: %v", err)
	}

	srcMajor := getMajorVersion(srcVersion)
	dstMajor := getMajorVersion(dstVersion)

	fmt.Printf("Source Redis: %s, Destination Redis: %s\n", srcVersion, dstVersion)

	// Use type-based sync if forced or if versions are incompatible
	if *forceTypeBased {
		fmt.Println("Using type-based sync (forced via -type-based flag)")
		useTypeBasedSync = true
	} else if srcMajor != dstMajor {
		fmt.Printf("Warning: Redis major version mismatch (%d vs %d), using type-based sync\n", srcMajor, dstMajor)
		useTypeBasedSync = true
	}

	// Get total key count
	totalKeys, err := srcClient.DBSize(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to get key count: %v", err)
	}
	fmt.Printf("Total keys to sync: %d\n", totalKeys)

	if totalKeys == 0 {
		fmt.Println("No keys to sync")
		return
	}

	// Create worker pool
	pool := pond.NewPool(*workers, pond.WithQueueSize(1024))
	defer pool.StopAndWait()

	// Channel for keys
	keyChan := make(chan string, 100)

	// Counter for completed keys
	var completed int64

	// Start scanner goroutine
	go func() {
		defer close(keyChan)
		var cursor uint64
		for {
			keys, nextCursor, err := srcClient.Scan(ctx, cursor, "*", 100).Result()
			if err != nil {
				log.Printf("Scan error: %v", err)
				return
			}
			for _, key := range keys {
				keyChan <- key
			}
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	}()

	// Submit jobs as keys come in
	for key := range keyChan {
		k := key // capture for closure
		pool.Submit(func() {
			if err := syncKey(ctx, srcClient, dstClient, k); err != nil {
				log.Printf("Failed to sync key %s: %v", k, err)
			}
			done := atomic.AddInt64(&completed, 1)
			fmt.Printf("\r%d of %d keys done", done, totalKeys)
		})
	}

	// Wait for all tasks to complete
	pool.StopAndWait()
	fmt.Printf("\r%d of %d keys done\n", atomic.LoadInt64(&completed), totalKeys)
	fmt.Println("Sync complete!")
}

func parseRedisURL(rawURL string) (*redis.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	opts := &redis.Options{
		Addr: u.Host,
	}

	// Handle TLS for rediss://
	if u.Scheme == "rediss" {
		opts.TLSConfig = &tls.Config{}
	}

	// Handle authentication
	if u.User != nil {
		password, hasPassword := u.User.Password()
		username := u.User.Username()
		if hasPassword {
			if username == "" {
				username = "default"
			}
			opts.Username = username
			opts.Password = password
		} else if username != "" {
			// Username only, treat as password with default user
			opts.Username = "default"
			opts.Password = username
		}
	}

	return redis.NewClient(opts), nil
}

func getRedisVersion(ctx context.Context, client *redis.Client) (string, error) {
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "redis_version:")), nil
		}
	}
	return "", fmt.Errorf("redis_version not found in INFO")
}

func getMajorVersion(version string) int {
	parts := strings.Split(version, ".")
	if len(parts) > 0 {
		major, _ := strconv.Atoi(parts[0])
		return major
	}
	return 0
}

func syncKey(ctx context.Context, src, dst *redis.Client, key string) error {
	// Get TTL
	ttl, err := src.PTTL(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("PTTL failed: %w", err)
	}

	// Use type-based sync if versions are incompatible
	if useTypeBasedSync {
		return syncKeyByType(ctx, src, dst, key, ttl)
	}

	// DUMP the key
	dump, err := src.Dump(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil // Key doesn't exist anymore
		}
		return fmt.Errorf("DUMP failed: %w", err)
	}

	// Convert TTL for RESTORE
	var restoreTTL time.Duration
	if ttl > 0 {
		restoreTTL = ttl
	} else {
		restoreTTL = 0 // No expiry or key has no TTL
	}

	// RESTORE to destination with REPLACE
	err = dst.RestoreReplace(ctx, key, restoreTTL, dump).Err()
	if err != nil {
		return fmt.Errorf("RESTORE failed: %w", err)
	}

	return nil
}

func syncKeyByType(ctx context.Context, src, dst *redis.Client, key string, ttl time.Duration) error {
	keyType, err := src.Type(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("TYPE failed: %w", err)
	}

	// Delete existing key first
	dst.Del(ctx, key)

	switch keyType {
	case "string":
		val, err := src.Get(ctx, key).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		if err := dst.Set(ctx, key, val, 0).Err(); err != nil {
			return err
		}

	case "list":
		vals, err := src.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return err
		}
		if len(vals) > 0 {
			if err := dst.RPush(ctx, key, vals).Err(); err != nil {
				return err
			}
		}

	case "set":
		vals, err := src.SMembers(ctx, key).Result()
		if err != nil {
			return err
		}
		if len(vals) > 0 {
			if err := dst.SAdd(ctx, key, vals).Err(); err != nil {
				return err
			}
		}

	case "zset":
		vals, err := src.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return err
		}
		if len(vals) > 0 {
			zs := make([]redis.Z, len(vals))
			for i, v := range vals {
				zs[i] = redis.Z{Score: v.Score, Member: v.Member}
			}
			if err := dst.ZAdd(ctx, key, zs...).Err(); err != nil {
				return err
			}
		}

	case "hash":
		vals, err := src.HGetAll(ctx, key).Result()
		if err != nil {
			return err
		}
		if len(vals) > 0 {
			if err := dst.HSet(ctx, key, vals).Err(); err != nil {
				return err
			}
		}

	case "stream":
		// Read all stream entries
		entries, err := src.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := dst.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				ID:     entry.ID,
				Values: entry.Values,
			}).Err(); err != nil {
				return err
			}
		}

	case "none":
		return nil // Key was deleted

	default:
		return fmt.Errorf("unsupported key type: %s", keyType)
	}

	// Set TTL if applicable
	if ttl > 0 {
		if err := dst.PExpire(ctx, key, ttl).Err(); err != nil {
			return err
		}
	}

	return nil
}
