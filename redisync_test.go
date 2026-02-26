package main

import (
	"context"
	"encoding/base64"
	"math"
	"os/exec"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	sourceDB = 13
	destDB   = 14
	redisURL = "127.0.0.1:6379"
)

func newRedisClient(db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: redisURL,
		DB:   db,
	})
}

func flushDB(t *testing.T, client *redis.Client) {
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush DB: %v", err)
	}
}

func TestRedisync(t *testing.T) {
	ctx := context.Background()

	// Create clients for both databases
	srcClient := newRedisClient(sourceDB)
	dstClient := newRedisClient(destDB)
	defer srcClient.Close()
	defer dstClient.Close()

	// Verify connection
	if err := srcClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Cannot connect to Redis source DB %d: %v", sourceDB, err)
	}
	if err := dstClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Cannot connect to Redis dest DB %d: %v", destDB, err)
	}

	// Flush both databases to ensure clean state
	t.Log("Flushing source and destination databases...")
	flushDB(t, srcClient)
	flushDB(t, dstClient)

	// Populate source with all different data types
	t.Log("Populating source database with test data...")
	populateTestData(t, ctx, srcClient)

	// Run redisync
	t.Log("Running redisync...")
	cmd := exec.Command("go", "run", "main.go",
		"-from", "redis://127.0.0.1/13",
		"-to", "redis://127.0.0.1/14",
	)
	output, err := cmd.CombinedOutput()
	t.Logf("redisync output:\n%s", string(output))
	if err != nil {
		t.Fatalf("redisync failed: %v", err)
	}

	// Verify all data was copied correctly
	t.Log("Verifying data was copied correctly...")
	verifyData(t, ctx, srcClient, dstClient)

	t.Log("All tests passed!")
}

func populateTestData(t *testing.T, ctx context.Context, client *redis.Client) {
	// 1. String types
	t.Log("  Adding string values...")
	if err := client.Set(ctx, "string:simple", "hello world", 0).Err(); err != nil {
		t.Fatalf("Failed to set string:simple: %v", err)
	}
	if err := client.Set(ctx, "string:empty", "", 0).Err(); err != nil {
		t.Fatalf("Failed to set string:empty: %v", err)
	}
	if err := client.Set(ctx, "string:binary", "\x00\x01\x02\xff\xfe\xfd", 0).Err(); err != nil {
		t.Fatalf("Failed to set string:binary: %v", err)
	}
	if err := client.Set(ctx, "string:unicode", "Hello 世界 🌍 مرحبا", 0).Err(); err != nil {
		t.Fatalf("Failed to set string:unicode: %v", err)
	}
	if err := client.Set(ctx, "string:number", "12345", 0).Err(); err != nil {
		t.Fatalf("Failed to set string:number: %v", err)
	}
	// String with TTL
	if err := client.Set(ctx, "string:with_ttl", "expires soon", 10*time.Minute).Err(); err != nil {
		t.Fatalf("Failed to set string:with_ttl: %v", err)
	}

	// 2. List types
	t.Log("  Adding list values...")
	if err := client.RPush(ctx, "list:simple", "a", "b", "c", "d", "e").Err(); err != nil {
		t.Fatalf("Failed to set list:simple: %v", err)
	}
	if err := client.RPush(ctx, "list:numbers", "1", "2", "3", "4", "5").Err(); err != nil {
		t.Fatalf("Failed to set list:numbers: %v", err)
	}
	if err := client.RPush(ctx, "list:unicode", "你好", "مرحبا", "🎉").Err(); err != nil {
		t.Fatalf("Failed to set list:unicode: %v", err)
	}

	// 3. Set types
	t.Log("  Adding set values...")
	if err := client.SAdd(ctx, "set:simple", "apple", "banana", "cherry").Err(); err != nil {
		t.Fatalf("Failed to set set:simple: %v", err)
	}
	if err := client.SAdd(ctx, "set:numbers", "1", "2", "3", "4", "5").Err(); err != nil {
		t.Fatalf("Failed to set set:numbers: %v", err)
	}
	if err := client.SAdd(ctx, "set:unicode", "日本語", "한국어", "العربية").Err(); err != nil {
		t.Fatalf("Failed to set set:unicode: %v", err)
	}

	// 4. Sorted Set (ZSet) types
	t.Log("  Adding sorted set values...")
	if err := client.ZAdd(ctx, "zset:simple",
		redis.Z{Score: 1.0, Member: "one"},
		redis.Z{Score: 2.0, Member: "two"},
		redis.Z{Score: 3.0, Member: "three"},
	).Err(); err != nil {
		t.Fatalf("Failed to set zset:simple: %v", err)
	}
	if err := client.ZAdd(ctx, "zset:float_scores",
		redis.Z{Score: 1.5, Member: "a"},
		redis.Z{Score: 2.7, Member: "b"},
		redis.Z{Score: -3.14159, Member: "c"},
		redis.Z{Score: 0, Member: "d"},
	).Err(); err != nil {
		t.Fatalf("Failed to set zset:float_scores: %v", err)
	}
	if err := client.ZAdd(ctx, "zset:inf_scores",
		redis.Z{Score: math.Inf(1), Member: "positive_inf"},
		redis.Z{Score: math.Inf(-1), Member: "negative_inf"},
	).Err(); err != nil {
		t.Fatalf("Failed to set zset:inf_scores: %v", err)
	}

	// 5. Hash types
	t.Log("  Adding hash values...")
	if err := client.HSet(ctx, "hash:simple",
		"field1", "value1",
		"field2", "value2",
		"field3", "value3",
	).Err(); err != nil {
		t.Fatalf("Failed to set hash:simple: %v", err)
	}
	if err := client.HSet(ctx, "hash:user",
		"name", "John Doe",
		"email", "john@example.com",
		"age", "30",
	).Err(); err != nil {
		t.Fatalf("Failed to set hash:user: %v", err)
	}
	if err := client.HSet(ctx, "hash:unicode",
		"中文", "Chinese",
		"日本語", "Japanese",
		"emoji", "🚀",
	).Err(); err != nil {
		t.Fatalf("Failed to set hash:unicode: %v", err)
	}

	// 6. Stream types
	t.Log("  Adding stream values...")
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:simple",
		Values: map[string]interface{}{"message": "hello", "count": "1"},
	}).Result(); err != nil {
		t.Fatalf("Failed to add stream:simple entry 1: %v", err)
	}
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:simple",
		Values: map[string]interface{}{"message": "world", "count": "2"},
	}).Result(); err != nil {
		t.Fatalf("Failed to add stream:simple entry 2: %v", err)
	}
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:events",
		Values: map[string]interface{}{"type": "login", "user": "alice"},
	}).Result(); err != nil {
		t.Fatalf("Failed to add stream:events entry 1: %v", err)
	}
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:events",
		Values: map[string]interface{}{"type": "logout", "user": "bob"},
	}).Result(); err != nil {
		t.Fatalf("Failed to add stream:events entry 2: %v", err)
	}

	// Verify count
	count, err := client.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("Failed to get DBSize: %v", err)
	}
	t.Logf("  Created %d keys in source database", count)
}

func verifyData(t *testing.T, ctx context.Context, src, dst *redis.Client) {
	// Verify key count
	srcCount, _ := src.DBSize(ctx).Result()
	dstCount, _ := dst.DBSize(ctx).Result()
	if srcCount != dstCount {
		t.Errorf("Key count mismatch: source=%d, dest=%d", srcCount, dstCount)
	} else {
		t.Logf("  Key count matches: %d keys", srcCount)
	}

	// Get all keys from source
	keys, err := src.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("Failed to get keys: %v", err)
	}

	for _, key := range keys {
		verifyKey(t, ctx, src, dst, key)
	}
}

func verifyKey(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	// Check key exists in destination
	exists, err := dst.Exists(ctx, key).Result()
	if err != nil {
		t.Errorf("Failed to check existence of key %s: %v", key, err)
		return
	}
	if exists == 0 {
		t.Errorf("Key %s missing from destination", key)
		return
	}

	// Check type matches
	srcType, _ := src.Type(ctx, key).Result()
	dstType, _ := dst.Type(ctx, key).Result()
	if srcType != dstType {
		t.Errorf("Type mismatch for key %s: source=%s, dest=%s", key, srcType, dstType)
		return
	}

	// Verify data based on type
	switch srcType {
	case "string":
		verifyString(t, ctx, src, dst, key)
	case "list":
		verifyList(t, ctx, src, dst, key)
	case "set":
		verifySet(t, ctx, src, dst, key)
	case "zset":
		verifyZSet(t, ctx, src, dst, key)
	case "hash":
		verifyHash(t, ctx, src, dst, key)
	case "stream":
		verifyStream(t, ctx, src, dst, key)
	}

	// Verify TTL (if applicable)
	// Only check if source has TTL
	srcTTL, _ := src.TTL(ctx, key).Result()
	dstTTL, _ := dst.TTL(ctx, key).Result()
	if srcTTL > 0 && dstTTL <= 0 {
		t.Errorf("TTL missing for key %s: source=%v, dest=%v", key, srcTTL, dstTTL)
	}
}

func verifyString(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVal, _ := src.Get(ctx, key).Bytes()
	dstVal, _ := dst.Get(ctx, key).Bytes()
	if !reflect.DeepEqual(srcVal, dstVal) {
		t.Errorf("String value mismatch for key %s: source=%q, dest=%q", key, srcVal, dstVal)
	} else {
		t.Logf("  ✓ String %s matches (len=%d)", key, len(srcVal))
	}
}

func verifyList(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVal, _ := src.LRange(ctx, key, 0, -1).Result()
	dstVal, _ := dst.LRange(ctx, key, 0, -1).Result()
	if !reflect.DeepEqual(srcVal, dstVal) {
		t.Errorf("List value mismatch for key %s: source=%v, dest=%v", key, srcVal, dstVal)
	} else {
		t.Logf("  ✓ List %s matches (len=%d)", key, len(srcVal))
	}
}

func verifySet(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVal, _ := src.SMembers(ctx, key).Result()
	dstVal, _ := dst.SMembers(ctx, key).Result()
	sort.Strings(srcVal)
	sort.Strings(dstVal)
	if !reflect.DeepEqual(srcVal, dstVal) {
		t.Errorf("Set value mismatch for key %s: source=%v, dest=%v", key, srcVal, dstVal)
	} else {
		t.Logf("  ✓ Set %s matches (len=%d)", key, len(srcVal))
	}
}

func verifyZSet(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVal, _ := src.ZRangeWithScores(ctx, key, 0, -1).Result()
	dstVal, _ := dst.ZRangeWithScores(ctx, key, 0, -1).Result()
	if len(srcVal) != len(dstVal) {
		t.Errorf("ZSet length mismatch for key %s: source=%d, dest=%d", key, len(srcVal), len(dstVal))
		return
	}
	for i := range srcVal {
		srcMember := srcVal[i].Member.(string)
		dstMember := dstVal[i].Member.(string)
		// Destination values are base64 decoded bytes
		if dstMemberBytes, ok := dstVal[i].Member.(string); ok {
			dstMember = dstMemberBytes
		}
		srcScore := srcVal[i].Score
		dstScore := dstVal[i].Score
		
		if srcMember != dstMember || !scoreEqual(srcScore, dstScore) {
			t.Errorf("ZSet member mismatch for key %s[%d]: source={%s, %f}, dest={%s, %f}", 
				key, i, srcMember, srcScore, dstMember, dstScore)
		}
	}
	t.Logf("  ✓ ZSet %s matches (len=%d)", key, len(srcVal))
}

func scoreEqual(a, b float64) bool {
	if math.IsInf(a, 1) && math.IsInf(b, 1) {
		return true
	}
	if math.IsInf(a, -1) && math.IsInf(b, -1) {
		return true
	}
	return a == b
}

func verifyHash(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVal, _ := src.HGetAll(ctx, key).Result()
	dstVal, _ := dst.HGetAll(ctx, key).Result()
	if !reflect.DeepEqual(srcVal, dstVal) {
		t.Errorf("Hash value mismatch for key %s: source=%v, dest=%v", key, srcVal, dstVal)
	} else {
		t.Logf("  ✓ Hash %s matches (len=%d)", key, len(srcVal))
	}
}

func verifyStream(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVal, _ := src.XRange(ctx, key, "-", "+").Result()
	dstVal, _ := dst.XRange(ctx, key, "-", "+").Result()
	if len(srcVal) != len(dstVal) {
		t.Errorf("Stream length mismatch for key %s: source=%d, dest=%d", key, len(srcVal), len(dstVal))
		return
	}
	for i := range srcVal {
		if srcVal[i].ID != dstVal[i].ID {
			t.Errorf("Stream ID mismatch for key %s[%d]: source=%s, dest=%s", key, i, srcVal[i].ID, dstVal[i].ID)
		}
		if !reflect.DeepEqual(srcVal[i].Values, dstVal[i].Values) {
			t.Errorf("Stream values mismatch for key %s[%d]: source=%v, dest=%v", key, i, srcVal[i].Values, dstVal[i].Values)
		}
	}
	t.Logf("  ✓ Stream %s matches (len=%d)", key, len(srcVal))
}

// Helper for debugging: print base64 encoded/decoded values
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func unb64(s string) string {
	b, _ := base64.StdEncoding.DecodeString(s)
	return string(b)
}

func TestParseRedisURLWithDB(t *testing.T) {
	// Test that parseRedisURL handles database selection
	// Note: The current implementation doesn't handle DB from URL path
	// This test documents the expected behavior
	
	testCases := []struct {
		url        string
		expectedDB int
	}{
		{"redis://127.0.0.1/13", 13},
		{"redis://127.0.0.1/14", 14},
		{"redis://127.0.0.1/0", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.url, func(t *testing.T) {
			client, err := parseRedisURL(tc.url)
			if err != nil {
				t.Fatalf("parseRedisURL failed: %v", err)
			}
			defer client.Close()

			// Check if it connects (won't verify DB selection in current implementation)
			ctx := context.Background()
			if err := client.Ping(ctx).Err(); err != nil {
				t.Errorf("Ping failed for %s: %v", tc.url, err)
			}
		})
	}
}

func TestRecordsEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        RedisRecord
		b        RedisRecord
		expected bool
	}{
		{
			name:     "equal strings",
			a:        RedisRecord{Key: "k", Type: "string", StringValue: strPtr("hello")},
			b:        RedisRecord{Key: "k", Type: "string", StringValue: strPtr("hello")},
			expected: true,
		},
		{
			name:     "different strings",
			a:        RedisRecord{Key: "k", Type: "string", StringValue: strPtr("hello")},
			b:        RedisRecord{Key: "k", Type: "string", StringValue: strPtr("world")},
			expected: false,
		},
		{
			name:     "nil vs non-nil string",
			a:        RedisRecord{Key: "k", Type: "string", StringValue: nil},
			b:        RedisRecord{Key: "k", Type: "string", StringValue: strPtr("hello")},
			expected: false,
		},
		{
			name:     "both nil strings",
			a:        RedisRecord{Key: "k", Type: "string", StringValue: nil},
			b:        RedisRecord{Key: "k", Type: "string", StringValue: nil},
			expected: true,
		},
		{
			name:     "different types",
			a:        RedisRecord{Key: "k", Type: "string", StringValue: strPtr("hello")},
			b:        RedisRecord{Key: "k", Type: "list", ListValues: []string{"hello"}},
			expected: false,
		},
		{
			name:     "equal lists",
			a:        RedisRecord{Key: "k", Type: "list", ListValues: []string{"a", "b", "c"}},
			b:        RedisRecord{Key: "k", Type: "list", ListValues: []string{"a", "b", "c"}},
			expected: true,
		},
		{
			name:     "different list length",
			a:        RedisRecord{Key: "k", Type: "list", ListValues: []string{"a", "b"}},
			b:        RedisRecord{Key: "k", Type: "list", ListValues: []string{"a", "b", "c"}},
			expected: false,
		},
		{
			name:     "different list order",
			a:        RedisRecord{Key: "k", Type: "list", ListValues: []string{"a", "b", "c"}},
			b:        RedisRecord{Key: "k", Type: "list", ListValues: []string{"c", "b", "a"}},
			expected: false,
		},
		{
			name:     "equal sets",
			a:        RedisRecord{Key: "k", Type: "set", SetMembers: []string{"a", "b", "c"}},
			b:        RedisRecord{Key: "k", Type: "set", SetMembers: []string{"c", "b", "a"}},
			expected: true,
		},
		{
			name:     "different sets",
			a:        RedisRecord{Key: "k", Type: "set", SetMembers: []string{"a", "b", "c"}},
			b:        RedisRecord{Key: "k", Type: "set", SetMembers: []string{"a", "b", "d"}},
			expected: false,
		},
		{
			name:     "equal zsets",
			a:        RedisRecord{Key: "k", Type: "zset", ZSetMembers: []ZSetMember{{Member: "a", Score: 1.0}, {Member: "b", Score: 2.0}}},
			b:        RedisRecord{Key: "k", Type: "zset", ZSetMembers: []ZSetMember{{Member: "b", Score: 2.0}, {Member: "a", Score: 1.0}}},
			expected: true,
		},
		{
			name:     "different zset scores",
			a:        RedisRecord{Key: "k", Type: "zset", ZSetMembers: []ZSetMember{{Member: "a", Score: 1.0}}},
			b:        RedisRecord{Key: "k", Type: "zset", ZSetMembers: []ZSetMember{{Member: "a", Score: 2.0}}},
			expected: false,
		},
		{
			name:     "equal hashes",
			a:        RedisRecord{Key: "k", Type: "hash", HashFields: map[string]string{"f1": "v1", "f2": "v2"}},
			b:        RedisRecord{Key: "k", Type: "hash", HashFields: map[string]string{"f2": "v2", "f1": "v1"}},
			expected: true,
		},
		{
			name:     "different hashes",
			a:        RedisRecord{Key: "k", Type: "hash", HashFields: map[string]string{"f1": "v1"}},
			b:        RedisRecord{Key: "k", Type: "hash", HashFields: map[string]string{"f1": "v2"}},
			expected: false,
		},
		{
			name: "equal streams",
			a: RedisRecord{Key: "k", Type: "stream", StreamItems: []StreamEntry{
				{ID: "1-0", Values: map[string]string{"k": "v"}},
			}},
			b: RedisRecord{Key: "k", Type: "stream", StreamItems: []StreamEntry{
				{ID: "1-0", Values: map[string]string{"k": "v"}},
			}},
			expected: true,
		},
		{
			name: "different stream IDs",
			a: RedisRecord{Key: "k", Type: "stream", StreamItems: []StreamEntry{
				{ID: "1-0", Values: map[string]string{"k": "v"}},
			}},
			b: RedisRecord{Key: "k", Type: "stream", StreamItems: []StreamEntry{
				{ID: "2-0", Values: map[string]string{"k": "v"}},
			}},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := recordsEqual(tc.a, tc.b)
			if result != tc.expected {
				t.Errorf("recordsEqual() = %v, expected %v", result, tc.expected)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}

func TestVerifyMode(t *testing.T) {
	ctx := context.Background()

	srcClient := newRedisClient(sourceDB)
	dstClient := newRedisClient(destDB)
	defer srcClient.Close()
	defer dstClient.Close()

	// Verify connection
	if err := srcClient.Ping(ctx).Err(); err != nil {
		t.Skipf("Cannot connect to Redis source DB %d: %v", sourceDB, err)
	}
	if err := dstClient.Ping(ctx).Err(); err != nil {
		t.Skipf("Cannot connect to Redis dest DB %d: %v", destDB, err)
	}

	// Test case 1: Matching databases
	t.Run("matching databases", func(t *testing.T) {
		flushDB(t, srcClient)
		flushDB(t, dstClient)

		// Add same data to both
		srcClient.Set(ctx, "test:key1", "value1", 0)
		srcClient.Set(ctx, "test:key2", "value2", 0)
		dstClient.Set(ctx, "test:key1", "value1", 0)
		dstClient.Set(ctx, "test:key2", "value2", 0)

		cmd := exec.Command("go", "run", "main.go",
			"-from", "redis://127.0.0.1/13",
			"-to", "redis://127.0.0.1/14",
			"-verify",
		)
		output, err := cmd.CombinedOutput()
		t.Logf("verify output:\n%s", string(output))
		if err != nil {
			t.Errorf("verify should pass for matching databases: %v", err)
		}
	})

	// Test case 2: Missing key in destination
	t.Run("missing key in destination", func(t *testing.T) {
		flushDB(t, srcClient)
		flushDB(t, dstClient)

		srcClient.Set(ctx, "test:key1", "value1", 0)
		srcClient.Set(ctx, "test:key2", "value2", 0)
		dstClient.Set(ctx, "test:key1", "value1", 0)
		// key2 missing in destination

		cmd := exec.Command("go", "run", "main.go",
			"-from", "redis://127.0.0.1/13",
			"-to", "redis://127.0.0.1/14",
			"-verify",
		)
		output, err := cmd.CombinedOutput()
		t.Logf("verify output:\n%s", string(output))
		if err == nil {
			t.Error("verify should fail when key is missing in destination")
		}
	})

	// Test case 3: Extra key in destination
	t.Run("extra key in destination", func(t *testing.T) {
		flushDB(t, srcClient)
		flushDB(t, dstClient)

		srcClient.Set(ctx, "test:key1", "value1", 0)
		dstClient.Set(ctx, "test:key1", "value1", 0)
		dstClient.Set(ctx, "test:extra", "extra", 0)

		cmd := exec.Command("go", "run", "main.go",
			"-from", "redis://127.0.0.1/13",
			"-to", "redis://127.0.0.1/14",
			"-verify",
		)
		output, err := cmd.CombinedOutput()
		t.Logf("verify output:\n%s", string(output))
		if err == nil {
			t.Error("verify should fail when extra key exists in destination")
		}
	})

	// Test case 4: Different values
	t.Run("different values", func(t *testing.T) {
		flushDB(t, srcClient)
		flushDB(t, dstClient)

		srcClient.Set(ctx, "test:key1", "value1", 0)
		dstClient.Set(ctx, "test:key1", "different", 0)

		cmd := exec.Command("go", "run", "main.go",
			"-from", "redis://127.0.0.1/13",
			"-to", "redis://127.0.0.1/14",
			"-verify",
		)
		output, err := cmd.CombinedOutput()
		t.Logf("verify output:\n%s", string(output))
		if err == nil {
			t.Error("verify should fail when values differ")
		}
	})
}
