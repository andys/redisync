package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/redis/go-redis/v9"
)

// SyncOptions configures the sync/verify operation
type SyncOptions struct {
	Workers       int
	ForceTypeBased bool
	VerifyMode    bool
	Output        io.Writer // For progress output, defaults to os.Stdout
}

// SyncResult contains the results of a sync/verify operation
type SyncResult struct {
	TotalKeys   int64
	Completed   int64
	Mismatched  int64  // Only used in verify mode
	SrcVersion  string
	DstVersion  string
	TypeBased   bool   // Whether type-based sync was used
}

// Sync copies all keys from src to dst Redis instances
func Sync(ctx context.Context, src, dst *redis.Client, opts SyncOptions) (*SyncResult, error) {
	if opts.Workers <= 0 {
		opts.Workers = 20
	}

	result := &SyncResult{}

	// Test connections
	if err := src.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to source Redis: %w", err)
	}
	if err := dst.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to destination Redis: %w", err)
	}

	// Get versions
	srcVersion, err := GetRedisVersion(ctx, src)
	if err != nil {
		return nil, fmt.Errorf("failed to get source Redis version: %w", err)
	}
	dstVersion, err := GetRedisVersion(ctx, dst)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination Redis version: %w", err)
	}
	result.SrcVersion = srcVersion
	result.DstVersion = dstVersion

	srcMajor := GetMajorVersion(srcVersion)
	dstMajor := GetMajorVersion(dstVersion)

	// Determine sync method
	useTypeBased := opts.ForceTypeBased || (srcMajor != dstMajor && !opts.VerifyMode)
	result.TypeBased = useTypeBased

	// Get total key count
	totalKeys, err := src.DBSize(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get key count: %w", err)
	}
	result.TotalKeys = totalKeys

	if totalKeys == 0 {
		return result, nil
	}

	// Create worker pool
	pool := pond.NewPool(opts.Workers, pond.WithQueueSize(1024))
	defer pool.StopAndWait()

	// Channel for keys
	keyChan := make(chan string, 100)

	// Counters
	var completed int64
	var mismatched int64

	// Start scanner goroutine
	go func() {
		defer close(keyChan)
		var cursor uint64
		for {
			keys, nextCursor, err := src.Scan(ctx, cursor, "*", 100).Result()
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
			if opts.VerifyMode {
				match, err := VerifyKey(ctx, src, dst, k)
				if err != nil {
					log.Printf("Failed to verify key %s: %v", k, err)
				} else if !match {
					atomic.AddInt64(&mismatched, 1)
				}
			} else {
				if err := SyncKey(ctx, src, dst, k, useTypeBased); err != nil {
					log.Printf("Failed to sync key %s: %v", k, err)
				}
			}
			atomic.AddInt64(&completed, 1)
		})
	}

	// Wait for all tasks to complete
	pool.StopAndWait()
	
	result.Completed = atomic.LoadInt64(&completed)
	result.Mismatched = atomic.LoadInt64(&mismatched)

	return result, nil
}

// ParseRedisURL parses a Redis URL and returns a client
func ParseRedisURL(rawURL string) (*redis.Client, error) {
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

	// Handle database number from path (e.g., /13)
	if u.Path != "" && u.Path != "/" {
		dbStr := strings.TrimPrefix(u.Path, "/")
		if db, err := strconv.Atoi(dbStr); err == nil {
			opts.DB = db
		}
	}

	return redis.NewClient(opts), nil
}

// GetRedisVersion returns the Redis server version
func GetRedisVersion(ctx context.Context, client *redis.Client) (string, error) {
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

// GetMajorVersion extracts the major version number from a version string
func GetMajorVersion(version string) int {
	parts := strings.Split(version, ".")
	if len(parts) > 0 {
		major, _ := strconv.Atoi(parts[0])
		return major
	}
	return 0
}

// SyncKey copies a single key from src to dst
func SyncKey(ctx context.Context, src, dst *redis.Client, key string, useTypeBased bool) error {
	// Get TTL
	ttl, err := src.PTTL(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("PTTL failed: %w", err)
	}

	// Use type-based sync if versions are incompatible
	if useTypeBased {
		return SyncKeyByType(ctx, src, dst, key, ttl)
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

// SyncKeyByType syncs a key using type-specific commands
func SyncKeyByType(ctx context.Context, src, dst *redis.Client, key string, ttl time.Duration) error {
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

// VerifyKey compares a key between source and destination
func VerifyKey(ctx context.Context, src, dst *redis.Client, key string) (bool, error) {
	// Check if key exists in both
	srcExists, err := src.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("EXISTS failed on source: %w", err)
	}
	dstExists, err := dst.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("EXISTS failed on destination: %w", err)
	}

	if srcExists == 0 && dstExists == 0 {
		return true, nil // Both don't have the key
	}
	if srcExists != dstExists {
		log.Printf("Key %s: exists mismatch (source=%d, dest=%d)", key, srcExists, dstExists)
		return false, nil
	}

	// Check type
	srcType, err := src.Type(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("TYPE failed on source: %w", err)
	}
	dstType, err := dst.Type(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("TYPE failed on destination: %w", err)
	}
	if srcType != dstType {
		log.Printf("Key %s: type mismatch (source=%s, dest=%s)", key, srcType, dstType)
		return false, nil
	}

	// Compare values based on type
	switch srcType {
	case "string":
		srcVal, err := src.Get(ctx, key).Result()
		if err != nil {
			return false, err
		}
		dstVal, err := dst.Get(ctx, key).Result()
		if err != nil {
			return false, err
		}
		if srcVal != dstVal {
			log.Printf("Key %s: string value mismatch", key)
			return false, nil
		}

	case "list":
		srcVals, err := src.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return false, err
		}
		dstVals, err := dst.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return false, err
		}
		if len(srcVals) != len(dstVals) {
			log.Printf("Key %s: list length mismatch (%d vs %d)", key, len(srcVals), len(dstVals))
			return false, nil
		}
		for i := range srcVals {
			if srcVals[i] != dstVals[i] {
				log.Printf("Key %s: list element %d mismatch", key, i)
				return false, nil
			}
		}

	case "set":
		srcVals, err := src.SMembers(ctx, key).Result()
		if err != nil {
			return false, err
		}
		dstVals, err := dst.SMembers(ctx, key).Result()
		if err != nil {
			return false, err
		}
		if len(srcVals) != len(dstVals) {
			log.Printf("Key %s: set size mismatch (%d vs %d)", key, len(srcVals), len(dstVals))
			return false, nil
		}
		srcSet := make(map[string]struct{}, len(srcVals))
		for _, v := range srcVals {
			srcSet[v] = struct{}{}
		}
		for _, v := range dstVals {
			if _, ok := srcSet[v]; !ok {
				log.Printf("Key %s: set member %s missing in source", key, v)
				return false, nil
			}
		}

	case "zset":
		srcVals, err := src.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return false, err
		}
		dstVals, err := dst.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return false, err
		}
		if len(srcVals) != len(dstVals) {
			log.Printf("Key %s: zset size mismatch (%d vs %d)", key, len(srcVals), len(dstVals))
			return false, nil
		}
		for i := range srcVals {
			if srcVals[i].Member != dstVals[i].Member || srcVals[i].Score != dstVals[i].Score {
				log.Printf("Key %s: zset entry %d mismatch", key, i)
				return false, nil
			}
		}

	case "hash":
		srcVals, err := src.HGetAll(ctx, key).Result()
		if err != nil {
			return false, err
		}
		dstVals, err := dst.HGetAll(ctx, key).Result()
		if err != nil {
			return false, err
		}
		if len(srcVals) != len(dstVals) {
			log.Printf("Key %s: hash size mismatch (%d vs %d)", key, len(srcVals), len(dstVals))
			return false, nil
		}
		for k, v := range srcVals {
			if dstVals[k] != v {
				log.Printf("Key %s: hash field %s mismatch", key, k)
				return false, nil
			}
		}

	case "stream":
		srcEntries, err := src.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return false, err
		}
		dstEntries, err := dst.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return false, err
		}
		if len(srcEntries) != len(dstEntries) {
			log.Printf("Key %s: stream length mismatch (%d vs %d)", key, len(srcEntries), len(dstEntries))
			return false, nil
		}
		for i := range srcEntries {
			if srcEntries[i].ID != dstEntries[i].ID {
				log.Printf("Key %s: stream entry %d ID mismatch", key, i)
				return false, nil
			}
			// Compare values map
			if len(srcEntries[i].Values) != len(dstEntries[i].Values) {
				log.Printf("Key %s: stream entry %d values count mismatch", key, i)
				return false, nil
			}
			for k, v := range srcEntries[i].Values {
				if dstEntries[i].Values[k] != v {
					log.Printf("Key %s: stream entry %d field %s mismatch", key, i, k)
					return false, nil
				}
			}
		}

	case "none":
		return true, nil
	}

	return true, nil
}
