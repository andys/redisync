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
	"time"

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
	flag.Parse()

	if *from == "" || *to == "" {
		fmt.Fprintln(os.Stderr, "Usage: redisync -from <source> -to <destination>")
		fmt.Fprintln(os.Stderr, "  source/destination can be:")
		fmt.Fprintln(os.Stderr, "    - Redis URL: redis://host:port/db or rediss://host:port/db")
		fmt.Fprintln(os.Stderr, "    - File path: /path/to/file.jsonl")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()

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

	// Scan all keys
	var cursor uint64
	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			return fmt.Errorf("scan error: %w", err)
		}

		for _, key := range keys {
			record, err := readKeyToRecord(ctx, client, key)
			if err != nil {
				log.Printf("Warning: failed to read key %s: %v", key, err)
				continue
			}
			if record != nil {
				out <- *record
				*count++
				fmt.Printf("\rRead %d of %d keys", *count, totalKeys)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

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

	for record := range in {
		if err := writeRecordToRedis(ctx, client, record); err != nil {
			return fmt.Errorf("failed to write key %s: %w", record.Key, err)
		}
		*count++
		fmt.Printf("\rWritten %d records", *count)
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
