package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/redis/go-redis/v9"
)

// RedisRecord represents a single Redis key with all its data in JSON format
type RedisRecord struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	TTLMs int64  `json:"ttl_ms,omitempty"` // TTL in milliseconds, 0 or negative means no expiry

	// Type-specific data (only one will be populated based on Type)
	StringValue *string           `json:"string_value,omitempty"`
	ListValues  []string          `json:"list_values,omitempty"`
	SetMembers  []string          `json:"set_members,omitempty"`
	ZSetMembers []ZSetMember      `json:"zset_members,omitempty"`
	HashFields  map[string]string `json:"hash_fields,omitempty"`
	StreamItems []StreamEntry     `json:"stream_items,omitempty"`
}

// ZSetMember represents a sorted set member with score
type ZSetMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

// StreamEntry represents a Redis stream entry
type StreamEntry struct {
	ID     string            `json:"id"`
	Values map[string]string `json:"values"`
}

func main() {
	from := flag.String("from", "", "Source: Redis URL (redis:// or rediss://) or file path")
	to := flag.String("to", "", "Destination: Redis URL (redis:// or rediss://) or file path")
	verify := flag.Bool("verify", false, "Verify mode: compare source and destination instead of syncing")
	flag.Parse()

	if *from == "" || *to == "" {
		fmt.Fprintln(os.Stderr, "Usage: redisync -from <source> -to <destination> [-verify]")
		fmt.Fprintln(os.Stderr, "  source/destination can be:")
		fmt.Fprintln(os.Stderr, "    - Redis URL: redis://host:port/db or rediss://host:port/db")
		fmt.Fprintln(os.Stderr, "    - File path: /path/to/file.jsonl")
		fmt.Fprintln(os.Stderr, "  -verify: compare source and destination, report differences")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()

	if *verify {
		if err := runVerify(ctx, *from, *to); err != nil {
			log.Fatalf("Verify error: %v", err)
		}
		return
	}

	// Channel for records
	recordChan := make(chan RedisRecord, 100)

	// Counter for records
	var produced, consumed int64

	// Determine source type and start reader
	srcIsRedis := isRedisURL(*from)
	dstIsRedis := isRedisURL(*to)

	// Start writer goroutine
	writerDone := make(chan error, 1)
	go func() {
		var err error
		if dstIsRedis {
			err = writeToRedis(ctx, *to, recordChan, &consumed)
		} else {
			err = writeToFile(*to, recordChan, &consumed)
		}
		writerDone <- err
	}()

	// Run reader
	var readerErr error
	if srcIsRedis {
		readerErr = readFromRedis(ctx, *from, recordChan, &produced)
	} else {
		readerErr = readFromFile(*from, recordChan, &produced)
	}

	if readerErr != nil {
		log.Fatalf("Reader error: %v", readerErr)
	}

	// Wait for writer to finish
	if err := <-writerDone; err != nil {
		log.Fatalf("Writer error: %v", err)
	}

	fmt.Printf("\nSync complete! Produced: %d, Consumed: %d\n", produced, consumed)
}

func isRedisURL(s string) bool {
	return strings.HasPrefix(s, "redis://") || strings.HasPrefix(s, "rediss://")
}

// runVerify compares source and destination, reporting any differences
func runVerify(ctx context.Context, from, to string) error {
	fmt.Println("Verify mode: comparing source and destination...")

	// Load source records into a map
	srcRecords := make(map[string]RedisRecord)
	srcChan := make(chan RedisRecord, 100)
	var srcCount int64

	srcIsRedis := isRedisURL(from)
	go func() {
		var err error
		if srcIsRedis {
			err = readFromRedis(ctx, from, srcChan, &srcCount)
		} else {
			err = readFromFile(from, srcChan, &srcCount)
		}
		if err != nil {
			log.Printf("Error reading source: %v", err)
		}
	}()

	for record := range srcChan {
		srcRecords[record.Key] = record
	}

	// Load destination records into a map
	dstRecords := make(map[string]RedisRecord)
	dstChan := make(chan RedisRecord, 100)
	var dstCount int64

	dstIsRedis := isRedisURL(to)
	go func() {
		var err error
		if dstIsRedis {
			err = readFromRedis(ctx, to, dstChan, &dstCount)
		} else {
			err = readFromFile(to, dstChan, &dstCount)
		}
		if err != nil {
			log.Printf("Error reading destination: %v", err)
		}
	}()

	for record := range dstChan {
		dstRecords[record.Key] = record
	}

	// Compare records
	var missingInDst, missingInSrc, different int
	var diffDetails []string

	// Check for keys in source but not in destination, or different
	for key, srcRec := range srcRecords {
		dstRec, exists := dstRecords[key]
		if !exists {
			missingInDst++
			diffDetails = append(diffDetails, fmt.Sprintf("  MISSING in destination: %s", key))
			continue
		}
		if !recordsEqual(srcRec, dstRec) {
			different++
			diffDetails = append(diffDetails, fmt.Sprintf("  DIFFERENT: %s", key))
		}
	}

	// Check for keys in destination but not in source
	for key := range dstRecords {
		if _, exists := srcRecords[key]; !exists {
			missingInSrc++
			diffDetails = append(diffDetails, fmt.Sprintf("  EXTRA in destination: %s", key))
		}
	}

	fmt.Printf("\n\nVerification Results:\n")
	fmt.Printf("  Source keys:      %d\n", len(srcRecords))
	fmt.Printf("  Destination keys: %d\n", len(dstRecords))
	fmt.Printf("  Missing in dest:  %d\n", missingInDst)
	fmt.Printf("  Extra in dest:    %d\n", missingInSrc)
	fmt.Printf("  Different values: %d\n", different)

	if len(diffDetails) > 0 {
		fmt.Println("\nDifferences:")
		for _, d := range diffDetails {
			fmt.Println(d)
		}
		return fmt.Errorf("verification failed: %d differences found", len(diffDetails))
	}

	fmt.Println("\nVerification PASSED: source and destination match!")
	return nil
}

// recordsEqual compares two RedisRecords for equality
func recordsEqual(a, b RedisRecord) bool {
	if a.Key != b.Key || a.Type != b.Type {
		return false
	}

	// Compare based on type
	switch a.Type {
	case "string":
		if a.StringValue == nil && b.StringValue == nil {
			return true
		}
		if a.StringValue == nil || b.StringValue == nil {
			return false
		}
		return *a.StringValue == *b.StringValue

	case "list":
		if len(a.ListValues) != len(b.ListValues) {
			return false
		}
		for i := range a.ListValues {
			if a.ListValues[i] != b.ListValues[i] {
				return false
			}
		}
		return true

	case "set":
		if len(a.SetMembers) != len(b.SetMembers) {
			return false
		}
		aSet := make(map[string]bool, len(a.SetMembers))
		for _, m := range a.SetMembers {
			aSet[m] = true
		}
		for _, m := range b.SetMembers {
			if !aSet[m] {
				return false
			}
		}
		return true

	case "zset":
		if len(a.ZSetMembers) != len(b.ZSetMembers) {
			return false
		}
		aMap := make(map[string]float64, len(a.ZSetMembers))
		for _, m := range a.ZSetMembers {
			aMap[m.Member] = m.Score
		}
		for _, m := range b.ZSetMembers {
			if score, exists := aMap[m.Member]; !exists || score != m.Score {
				return false
			}
		}
		return true

	case "hash":
		if len(a.HashFields) != len(b.HashFields) {
			return false
		}
		for k, v := range a.HashFields {
			if bv, exists := b.HashFields[k]; !exists || bv != v {
				return false
			}
		}
		return true

	case "stream":
		if len(a.StreamItems) != len(b.StreamItems) {
			return false
		}
		for i := range a.StreamItems {
			if a.StreamItems[i].ID != b.StreamItems[i].ID {
				return false
			}
			if len(a.StreamItems[i].Values) != len(b.StreamItems[i].Values) {
				return false
			}
			for k, v := range a.StreamItems[i].Values {
				if bv, exists := b.StreamItems[i].Values[k]; !exists || bv != v {
					return false
				}
			}
		}
		return true
	}

	return false
}

// readFromRedis reads all keys from Redis and sends them as RedisRecords to the channel
func readFromRedis(ctx context.Context, rawURL string, out chan<- RedisRecord, count *int64) error {
	defer close(out)

	client, err := parseRedisURL(rawURL)
	if err != nil {
		return fmt.Errorf("failed to parse Redis URL: %w", err)
	}
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	version, err := getRedisVersion(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to get Redis version: %w", err)
	}
	fmt.Printf("Source Redis version: %s\n", version)

	totalKeys, err := client.DBSize(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to get key count: %w", err)
	}
	fmt.Printf("Total keys to read: %d\n", totalKeys)

	if totalKeys == 0 {
		fmt.Println("No keys to sync")
		return nil
	}

	// Create a pool of workers for reading keys concurrently
	pool := pond.NewPool(20)
	defer pool.StopAndWait()

	var atomicCount int64

	// Scan all keys
	var cursor uint64
	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			return fmt.Errorf("scan error: %w", err)
		}

		for _, key := range keys {
			key := key // capture for closure
			pool.Submit(func() {
				record, err := readKeyToRecord(ctx, client, key)
				if err != nil {
					log.Printf("Warning: failed to read key %s: %v", key, err)
					return
				}
				if record != nil {
					out <- *record
					newCount := atomic.AddInt64(&atomicCount, 1)
					fmt.Printf("\rRead %d of %d keys", newCount, totalKeys)
				}
			})
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// Wait for all workers to finish
	pool.StopAndWait()
	*count = atomic.LoadInt64(&atomicCount)

	fmt.Println()
	return nil
}

// readKeyToRecord reads a single key from Redis and converts it to a RedisRecord
func readKeyToRecord(ctx context.Context, client *redis.Client, key string) (*RedisRecord, error) {
	keyType, err := client.Type(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("TYPE failed: %w", err)
	}

	if keyType == "none" {
		return nil, nil // Key was deleted
	}

	// Get TTL
	ttl, err := client.PTTL(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("PTTL failed: %w", err)
	}

	record := &RedisRecord{
		Key:   key,
		Type:  keyType,
		TTLMs: ttl.Milliseconds(),
	}

	switch keyType {
	case "string":
		val, err := client.Get(ctx, key).Bytes()
		if err != nil {
			if err == redis.Nil {
				return nil, nil
			}
			return nil, err
		}
		// Base64 encode to preserve binary data
		encoded := base64.StdEncoding.EncodeToString(val)
		record.StringValue = &encoded

	case "list":
		vals, err := client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		// Base64 encode each value
		record.ListValues = make([]string, len(vals))
		for i, v := range vals {
			record.ListValues[i] = base64.StdEncoding.EncodeToString([]byte(v))
		}

	case "set":
		vals, err := client.SMembers(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		record.SetMembers = make([]string, len(vals))
		for i, v := range vals {
			record.SetMembers[i] = base64.StdEncoding.EncodeToString([]byte(v))
		}

	case "zset":
		vals, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		record.ZSetMembers = make([]ZSetMember, len(vals))
		for i, v := range vals {
			record.ZSetMembers[i] = ZSetMember{
				Member: base64.StdEncoding.EncodeToString([]byte(v.Member.(string))),
				Score:  v.Score,
			}
		}

	case "hash":
		vals, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		record.HashFields = make(map[string]string, len(vals))
		for k, v := range vals {
			// Base64 encode both key and value
			encodedKey := base64.StdEncoding.EncodeToString([]byte(k))
			encodedVal := base64.StdEncoding.EncodeToString([]byte(v))
			record.HashFields[encodedKey] = encodedVal
		}

	case "stream":
		entries, err := client.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return nil, err
		}
		record.StreamItems = make([]StreamEntry, len(entries))
		for i, entry := range entries {
			values := make(map[string]string, len(entry.Values))
			for k, v := range entry.Values {
				encodedKey := base64.StdEncoding.EncodeToString([]byte(k))
				encodedVal := base64.StdEncoding.EncodeToString([]byte(v.(string)))
				values[encodedKey] = encodedVal
			}
			record.StreamItems[i] = StreamEntry{
				ID:     entry.ID,
				Values: values,
			}
		}

	default:
		return nil, fmt.Errorf("unsupported key type: %s", keyType)
	}

	return record, nil
}

// readFromFile reads RedisRecords from a JSON lines file
func readFromFile(path string, out chan<- RedisRecord, count *int64) error {
	defer close(out)

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fmt.Printf("Reading from file: %s\n", path)

	scanner := bufio.NewScanner(file)
	// Increase buffer size for large records
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record RedisRecord
		if err := json.Unmarshal(line, &record); err != nil {
			log.Printf("Warning: failed to parse JSON line: %v", err)
			continue
		}

		out <- record
		*count++
		fmt.Printf("\rRead %d records", *count)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	fmt.Println()
	return nil
}

// writeToRedis writes RedisRecords to a Redis server
func writeToRedis(ctx context.Context, rawURL string, in <-chan RedisRecord, count *int64) error {
	client, err := parseRedisURL(rawURL)
	if err != nil {
		return fmt.Errorf("failed to parse Redis URL: %w", err)
	}
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	version, err := getRedisVersion(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to get Redis version: %w", err)
	}
	fmt.Printf("Destination Redis version: %s\n", version)

	// Create a pool of workers for writing concurrently
	pool := pond.NewPool(20)
	var atomicCount int64
	var writeErr atomic.Value

	for record := range in {
		record := record // capture for closure
		pool.Submit(func() {
			if err := writeRecordToRedis(ctx, client, record); err != nil {
				writeErr.Store(fmt.Errorf("failed to write key %s: %w", record.Key, err))
				return
			}
			newCount := atomic.AddInt64(&atomicCount, 1)
			fmt.Printf("\rWritten %d records", newCount)
		})
	}

	pool.StopAndWait()
	*count = atomic.LoadInt64(&atomicCount)

	if err := writeErr.Load(); err != nil {
		return err.(error)
	}

	return nil
}

// writeRecordToRedis writes a single RedisRecord to Redis
func writeRecordToRedis(ctx context.Context, client *redis.Client, record RedisRecord) error {
	// Delete existing key first
	client.Del(ctx, record.Key)

	switch record.Type {
	case "string":
		if record.StringValue == nil {
			return nil
		}
		val, err := base64.StdEncoding.DecodeString(*record.StringValue)
		if err != nil {
			return fmt.Errorf("failed to decode string value: %w", err)
		}
		if err := client.Set(ctx, record.Key, val, 0).Err(); err != nil {
			return err
		}

	case "list":
		if len(record.ListValues) == 0 {
			return nil
		}
		vals := make([]interface{}, len(record.ListValues))
		for i, v := range record.ListValues {
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return fmt.Errorf("failed to decode list value: %w", err)
			}
			vals[i] = decoded
		}
		if err := client.RPush(ctx, record.Key, vals...).Err(); err != nil {
			return err
		}

	case "set":
		if len(record.SetMembers) == 0 {
			return nil
		}
		vals := make([]interface{}, len(record.SetMembers))
		for i, v := range record.SetMembers {
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return fmt.Errorf("failed to decode set member: %w", err)
			}
			vals[i] = decoded
		}
		if err := client.SAdd(ctx, record.Key, vals...).Err(); err != nil {
			return err
		}

	case "zset":
		if len(record.ZSetMembers) == 0 {
			return nil
		}
		zs := make([]redis.Z, len(record.ZSetMembers))
		for i, m := range record.ZSetMembers {
			decoded, err := base64.StdEncoding.DecodeString(m.Member)
			if err != nil {
				return fmt.Errorf("failed to decode zset member: %w", err)
			}
			zs[i] = redis.Z{Score: m.Score, Member: decoded}
		}
		if err := client.ZAdd(ctx, record.Key, zs...).Err(); err != nil {
			return err
		}

	case "hash":
		if len(record.HashFields) == 0 {
			return nil
		}
		vals := make(map[string]interface{}, len(record.HashFields))
		for k, v := range record.HashFields {
			decodedKey, err := base64.StdEncoding.DecodeString(k)
			if err != nil {
				return fmt.Errorf("failed to decode hash key: %w", err)
			}
			decodedVal, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return fmt.Errorf("failed to decode hash value: %w", err)
			}
			vals[string(decodedKey)] = decodedVal
		}
		if err := client.HSet(ctx, record.Key, vals).Err(); err != nil {
			return err
		}

	case "stream":
		if len(record.StreamItems) == 0 {
			return nil
		}
		for _, entry := range record.StreamItems {
			values := make(map[string]interface{}, len(entry.Values))
			for k, v := range entry.Values {
				decodedKey, err := base64.StdEncoding.DecodeString(k)
				if err != nil {
					return fmt.Errorf("failed to decode stream key: %w", err)
				}
				decodedVal, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					return fmt.Errorf("failed to decode stream value: %w", err)
				}
				values[string(decodedKey)] = decodedVal
			}
			if err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: record.Key,
				ID:     entry.ID,
				Values: values,
			}).Err(); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("unsupported key type: %s", record.Type)
	}

	// Set TTL if applicable (positive TTL means key has expiry)
	if record.TTLMs > 0 {
		if err := client.PExpire(ctx, record.Key, time.Duration(record.TTLMs)*time.Millisecond).Err(); err != nil {
			return err
		}
	}

	return nil
}

// writeToFile writes RedisRecords to a JSON lines file
func writeToFile(path string, in <-chan RedisRecord, count *int64) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	fmt.Printf("Writing to file: %s\n", path)

	for record := range in {
		data, err := json.Marshal(record)
		if err != nil {
			log.Printf("Warning: failed to marshal record for key %s: %v", record.Key, err)
			continue
		}

		if _, err := writer.Write(data); err != nil {
			return fmt.Errorf("write error: %w", err)
		}
		if _, err := writer.WriteString("\n"); err != nil {
			return fmt.Errorf("write error: %w", err)
		}

		*count++
		fmt.Printf("\rWritten %d records", *count)
	}

	fmt.Println()
	return nil
}

func parseRedisURL(rawURL string) (*redis.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Handle host:port, defaulting to port 6379 if not specified
	host := u.Host
	if !strings.Contains(host, ":") {
		host = host + ":6379"
	}

	opts := &redis.Options{
		Addr: host,
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

	// Handle database selection from path (e.g., /13)
	if u.Path != "" && u.Path != "/" {
		dbStr := strings.TrimPrefix(u.Path, "/")
		if dbStr != "" {
			db, err := parseDBNumber(dbStr)
			if err != nil {
				return nil, fmt.Errorf("invalid database number: %w", err)
			}
			opts.DB = db
		}
	}

	return redis.NewClient(opts), nil
}

func parseDBNumber(s string) (int, error) {
	var db int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid database number: %s", s)
		}
		db = db*10 + int(c-'0')
	}
	return db, nil
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
