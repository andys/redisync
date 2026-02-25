package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	srcDB = 13
	dstDB = 14
)

func getClient(db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   db,
	})
}

func TestRedisync(t *testing.T) {
	ctx := context.Background()

	src := getClient(srcDB)
	dst := getClient(dstDB)
	defer src.Close()
	defer dst.Close()

	// Test connection
	if err := src.Ping(ctx).Err(); err != nil {
		t.Fatalf("Cannot connect to Redis (db %d): %v", srcDB, err)
	}
	if err := dst.Ping(ctx).Err(); err != nil {
		t.Fatalf("Cannot connect to Redis (db %d): %v", dstDB, err)
	}

	// Clean both databases
	t.Log("Cleaning databases...")
	if err := src.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush source db: %v", err)
	}
	if err := dst.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush destination db: %v", err)
	}

	// Populate source with all data types
	t.Log("Populating source database with test data...")
	populateTestData(t, ctx, src)

	// Get key count
	keyCount, err := src.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("Failed to get key count: %v", err)
	}
	t.Logf("Source database populated with %d keys", keyCount)

	// Run sync using the Sync function directly
	t.Log("Running sync from db 13 to db 14...")
	result, err := Sync(ctx, src, dst, SyncOptions{
		Workers:        10,
		ForceTypeBased: false,
		VerifyMode:     false,
	})
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	t.Logf("Sync completed: %d keys synced (Redis %s -> %s)", result.Completed, result.SrcVersion, result.DstVersion)

	// Verify key count
	dstKeyCount, err := dst.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("Failed to get destination key count: %v", err)
	}
	if dstKeyCount != keyCount {
		t.Errorf("Key count mismatch: source=%d, dest=%d", keyCount, dstKeyCount)
	}

	// Run verification using the Sync function in verify mode
	t.Log("Running verification...")
	verifyResult, err := Sync(ctx, src, dst, SyncOptions{
		Workers:    10,
		VerifyMode: true,
	})
	if err != nil {
		t.Fatalf("Verification failed: %v", err)
	}
	if verifyResult.Mismatched > 0 {
		t.Errorf("Verification found %d mismatched keys", verifyResult.Mismatched)
	} else {
		t.Logf("Verification passed: %d keys match", verifyResult.Completed)
	}

	// Manual verification of each type
	t.Log("Performing manual verification...")

	// Verify strings
	verifyString(t, ctx, src, dst, "string:simple")
	verifyString(t, ctx, src, dst, "string:empty")
	verifyString(t, ctx, src, dst, "string:binary")
	verifyString(t, ctx, src, dst, "string:unicode")
	verifyString(t, ctx, src, dst, "string:number")
	verifyString(t, ctx, src, dst, "string:with_ttl")

	// Verify TTL exists on the TTL key
	ttl, err := dst.TTL(ctx, "string:with_ttl").Result()
	if err != nil {
		t.Errorf("Failed to get TTL: %v", err)
	} else if ttl <= 0 {
		t.Errorf("TTL not preserved for string:with_ttl")
	} else {
		t.Logf("TTL preserved: %v", ttl)
	}

	// Verify lists
	verifyList(t, ctx, src, dst, "list:simple")
	verifyList(t, ctx, src, dst, "list:numbers")
	verifyList(t, ctx, src, dst, "list:single")

	// Verify sets
	verifySet(t, ctx, src, dst, "set:simple")
	verifySet(t, ctx, src, dst, "set:numbers")
	verifySet(t, ctx, src, dst, "set:single")

	// Verify sorted sets
	verifyZSet(t, ctx, src, dst, "zset:simple")
	verifyZSet(t, ctx, src, dst, "zset:scores")
	verifyZSet(t, ctx, src, dst, "zset:negative")

	// Verify hashes
	verifyHash(t, ctx, src, dst, "hash:simple")
	verifyHash(t, ctx, src, dst, "hash:user")
	verifyHash(t, ctx, src, dst, "hash:empty_value")

	// Verify streams
	verifyStream(t, ctx, src, dst, "stream:events")

	t.Log("All verifications passed!")
}

func populateTestData(t *testing.T, ctx context.Context, src *redis.Client) {
	// 1. String types
	if err := src.Set(ctx, "string:simple", "hello world", 0).Err(); err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}
	if err := src.Set(ctx, "string:empty", "", 0).Err(); err != nil {
		t.Fatalf("Failed to set empty string: %v", err)
	}
	if err := src.Set(ctx, "string:binary", "\x00\x01\x02\x03\xff", 0).Err(); err != nil {
		t.Fatalf("Failed to set binary string: %v", err)
	}
	if err := src.Set(ctx, "string:unicode", "こんにちは世界 🌍", 0).Err(); err != nil {
		t.Fatalf("Failed to set unicode string: %v", err)
	}
	if err := src.Set(ctx, "string:number", "12345", 0).Err(); err != nil {
		t.Fatalf("Failed to set number string: %v", err)
	}
	if err := src.Set(ctx, "string:with_ttl", "expires soon", 1*time.Hour).Err(); err != nil {
		t.Fatalf("Failed to set string with TTL: %v", err)
	}

	// 2. List types
	if err := src.RPush(ctx, "list:simple", "a", "b", "c", "d", "e").Err(); err != nil {
		t.Fatalf("Failed to push list: %v", err)
	}
	if err := src.RPush(ctx, "list:numbers", "1", "2", "3", "4", "5").Err(); err != nil {
		t.Fatalf("Failed to push number list: %v", err)
	}
	if err := src.RPush(ctx, "list:single", "only_one").Err(); err != nil {
		t.Fatalf("Failed to push single item list: %v", err)
	}

	// 3. Set types
	if err := src.SAdd(ctx, "set:simple", "apple", "banana", "cherry").Err(); err != nil {
		t.Fatalf("Failed to add set: %v", err)
	}
	if err := src.SAdd(ctx, "set:numbers", "1", "2", "3", "4", "5").Err(); err != nil {
		t.Fatalf("Failed to add number set: %v", err)
	}
	if err := src.SAdd(ctx, "set:single", "lonely").Err(); err != nil {
		t.Fatalf("Failed to add single member set: %v", err)
	}

	// 4. Sorted Set (ZSet) types
	if err := src.ZAdd(ctx, "zset:simple",
		redis.Z{Score: 1.0, Member: "one"},
		redis.Z{Score: 2.0, Member: "two"},
		redis.Z{Score: 3.0, Member: "three"},
	).Err(); err != nil {
		t.Fatalf("Failed to add zset: %v", err)
	}
	if err := src.ZAdd(ctx, "zset:scores",
		redis.Z{Score: 100.5, Member: "alice"},
		redis.Z{Score: 200.75, Member: "bob"},
		redis.Z{Score: 150.25, Member: "charlie"},
	).Err(); err != nil {
		t.Fatalf("Failed to add score zset: %v", err)
	}
	if err := src.ZAdd(ctx, "zset:negative",
		redis.Z{Score: -10.0, Member: "neg"},
		redis.Z{Score: 0.0, Member: "zero"},
		redis.Z{Score: 10.0, Member: "pos"},
	).Err(); err != nil {
		t.Fatalf("Failed to add negative score zset: %v", err)
	}

	// 5. Hash types
	if err := src.HSet(ctx, "hash:simple",
		"field1", "value1",
		"field2", "value2",
		"field3", "value3",
	).Err(); err != nil {
		t.Fatalf("Failed to set hash: %v", err)
	}
	if err := src.HSet(ctx, "hash:user",
		"name", "John Doe",
		"email", "john@example.com",
		"age", "30",
	).Err(); err != nil {
		t.Fatalf("Failed to set user hash: %v", err)
	}
	if err := src.HSet(ctx, "hash:empty_value",
		"key", "",
	).Err(); err != nil {
		t.Fatalf("Failed to set hash with empty value: %v", err)
	}

	// 6. Stream types
	if _, err := src.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:events",
		Values: map[string]interface{}{"event": "login", "user": "alice"},
	}).Result(); err != nil {
		t.Fatalf("Failed to add stream entry: %v", err)
	}
	if _, err := src.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:events",
		Values: map[string]interface{}{"event": "purchase", "user": "bob", "amount": "100"},
	}).Result(); err != nil {
		t.Fatalf("Failed to add stream entry: %v", err)
	}
	if _, err := src.XAdd(ctx, &redis.XAddArgs{
		Stream: "stream:events",
		Values: map[string]interface{}{"event": "logout", "user": "alice"},
	}).Result(); err != nil {
		t.Fatalf("Failed to add stream entry: %v", err)
	}
}

func verifyString(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVal, err := src.Get(ctx, key).Result()
	if err != nil {
		t.Errorf("Failed to get source string %s: %v", key, err)
		return
	}
	dstVal, err := dst.Get(ctx, key).Result()
	if err != nil {
		t.Errorf("Failed to get dest string %s: %v", key, err)
		return
	}
	if srcVal != dstVal {
		t.Errorf("String mismatch for %s: src=%q, dst=%q", key, srcVal, dstVal)
	}
}

func verifyList(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVals, err := src.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Errorf("Failed to get source list %s: %v", key, err)
		return
	}
	dstVals, err := dst.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Errorf("Failed to get dest list %s: %v", key, err)
		return
	}
	if len(srcVals) != len(dstVals) {
		t.Errorf("List length mismatch for %s: src=%d, dst=%d", key, len(srcVals), len(dstVals))
		return
	}
	for i := range srcVals {
		if srcVals[i] != dstVals[i] {
			t.Errorf("List element mismatch for %s at index %d: src=%q, dst=%q", key, i, srcVals[i], dstVals[i])
		}
	}
}

func verifySet(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVals, err := src.SMembers(ctx, key).Result()
	if err != nil {
		t.Errorf("Failed to get source set %s: %v", key, err)
		return
	}
	dstVals, err := dst.SMembers(ctx, key).Result()
	if err != nil {
		t.Errorf("Failed to get dest set %s: %v", key, err)
		return
	}
	if len(srcVals) != len(dstVals) {
		t.Errorf("Set size mismatch for %s: src=%d, dst=%d", key, len(srcVals), len(dstVals))
		return
	}
	srcSet := make(map[string]struct{}, len(srcVals))
	for _, v := range srcVals {
		srcSet[v] = struct{}{}
	}
	for _, v := range dstVals {
		if _, ok := srcSet[v]; !ok {
			t.Errorf("Set member %q in dest but not in source for %s", v, key)
		}
	}
}

func verifyZSet(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVals, err := src.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		t.Errorf("Failed to get source zset %s: %v", key, err)
		return
	}
	dstVals, err := dst.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		t.Errorf("Failed to get dest zset %s: %v", key, err)
		return
	}
	if len(srcVals) != len(dstVals) {
		t.Errorf("ZSet size mismatch for %s: src=%d, dst=%d", key, len(srcVals), len(dstVals))
		return
	}
	for i := range srcVals {
		if srcVals[i].Member != dstVals[i].Member || srcVals[i].Score != dstVals[i].Score {
			t.Errorf("ZSet entry mismatch for %s at index %d", key, i)
		}
	}
}

func verifyHash(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcVals, err := src.HGetAll(ctx, key).Result()
	if err != nil {
		t.Errorf("Failed to get source hash %s: %v", key, err)
		return
	}
	dstVals, err := dst.HGetAll(ctx, key).Result()
	if err != nil {
		t.Errorf("Failed to get dest hash %s: %v", key, err)
		return
	}
	if len(srcVals) != len(dstVals) {
		t.Errorf("Hash size mismatch for %s: src=%d, dst=%d", key, len(srcVals), len(dstVals))
		return
	}
	for k, v := range srcVals {
		if dstVals[k] != v {
			t.Errorf("Hash field mismatch for %s.%s: src=%q, dst=%q", key, k, v, dstVals[k])
		}
	}
}

func verifyStream(t *testing.T, ctx context.Context, src, dst *redis.Client, key string) {
	srcEntries, err := src.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		t.Errorf("Failed to get source stream %s: %v", key, err)
		return
	}
	dstEntries, err := dst.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		t.Errorf("Failed to get dest stream %s: %v", key, err)
		return
	}
	if len(srcEntries) != len(dstEntries) {
		t.Errorf("Stream length mismatch for %s: src=%d, dst=%d", key, len(srcEntries), len(dstEntries))
		return
	}
	for i := range srcEntries {
		if srcEntries[i].ID != dstEntries[i].ID {
			t.Errorf("Stream entry ID mismatch for %s at index %d: src=%s, dst=%s", key, i, srcEntries[i].ID, dstEntries[i].ID)
		}
		for k, v := range srcEntries[i].Values {
			if dstEntries[i].Values[k] != v {
				t.Errorf("Stream entry value mismatch for %s at index %d, field %s", key, i, k)
			}
		}
	}
}

// TestTypeBasedSync tests the type-based sync method
func TestTypeBasedSync(t *testing.T) {
	ctx := context.Background()

	src := getClient(srcDB)
	dst := getClient(dstDB)
	defer src.Close()
	defer dst.Close()

	// Clean and setup minimal test data
	if err := src.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush source: %v", err)
	}
	if err := dst.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush dest: %v", err)
	}

	// Add one key of each type
	src.Set(ctx, "type:string", "test", 0)
	src.RPush(ctx, "type:list", "a", "b", "c")
	src.SAdd(ctx, "type:set", "x", "y", "z")
	src.ZAdd(ctx, "type:zset", redis.Z{Score: 1, Member: "m1"})
	src.HSet(ctx, "type:hash", "f1", "v1")

	// Run sync with type-based mode forced
	result, err := Sync(ctx, src, dst, SyncOptions{
		Workers:        5,
		ForceTypeBased: true,
		VerifyMode:     false,
	})
	if err != nil {
		t.Fatalf("Type-based sync failed: %v", err)
	}
	t.Logf("Type-based sync completed: %d keys synced", result.Completed)

	// Verify
	verifyResult, err := Sync(ctx, src, dst, SyncOptions{
		Workers:    5,
		VerifyMode: true,
	})
	if err != nil {
		t.Fatalf("Verification failed: %v", err)
	}
	if verifyResult.Mismatched > 0 {
		t.Errorf("Verification found %d mismatched keys", verifyResult.Mismatched)
	}

	t.Log("Type-based sync verified successfully!")
}

// TestSyncKey tests individual key sync operations
func TestSyncKey(t *testing.T) {
	ctx := context.Background()

	src := getClient(srcDB)
	dst := getClient(dstDB)
	defer src.Close()
	defer dst.Close()

	// Clean databases
	src.FlushDB(ctx)
	dst.FlushDB(ctx)

	// Test string sync
	t.Run("String", func(t *testing.T) {
		src.Set(ctx, "test:string", "hello", 0)
		if err := SyncKey(ctx, src, dst, "test:string", false); err != nil {
			t.Fatalf("SyncKey failed: %v", err)
		}
		val, _ := dst.Get(ctx, "test:string").Result()
		if val != "hello" {
			t.Errorf("Expected 'hello', got '%s'", val)
		}
	})

	// Test list sync
	t.Run("List", func(t *testing.T) {
		src.RPush(ctx, "test:list", "a", "b", "c")
		if err := SyncKey(ctx, src, dst, "test:list", false); err != nil {
			t.Fatalf("SyncKey failed: %v", err)
		}
		vals, _ := dst.LRange(ctx, "test:list", 0, -1).Result()
		if len(vals) != 3 || vals[0] != "a" {
			t.Errorf("List sync failed: %v", vals)
		}
	})

	// Test hash sync with type-based method
	t.Run("HashTypeBased", func(t *testing.T) {
		src.HSet(ctx, "test:hash", "key1", "val1", "key2", "val2")
		if err := SyncKey(ctx, src, dst, "test:hash", true); err != nil {
			t.Fatalf("SyncKey (type-based) failed: %v", err)
		}
		vals, _ := dst.HGetAll(ctx, "test:hash").Result()
		if vals["key1"] != "val1" || vals["key2"] != "val2" {
			t.Errorf("Hash sync failed: %v", vals)
		}
	})
}

// TestVerifyKey tests the key verification function
func TestVerifyKey(t *testing.T) {
	ctx := context.Background()

	src := getClient(srcDB)
	dst := getClient(dstDB)
	defer src.Close()
	defer dst.Close()

	// Clean databases
	src.FlushDB(ctx)
	dst.FlushDB(ctx)

	// Test matching strings
	t.Run("MatchingStrings", func(t *testing.T) {
		src.Set(ctx, "verify:match", "same", 0)
		dst.Set(ctx, "verify:match", "same", 0)
		match, err := VerifyKey(ctx, src, dst, "verify:match")
		if err != nil {
			t.Fatalf("VerifyKey failed: %v", err)
		}
		if !match {
			t.Error("Expected match=true for identical strings")
		}
	})

	// Test mismatched strings
	t.Run("MismatchedStrings", func(t *testing.T) {
		src.Set(ctx, "verify:mismatch", "value1", 0)
		dst.Set(ctx, "verify:mismatch", "value2", 0)
		match, err := VerifyKey(ctx, src, dst, "verify:mismatch")
		if err != nil {
			t.Fatalf("VerifyKey failed: %v", err)
		}
		if match {
			t.Error("Expected match=false for different strings")
		}
	})

	// Test missing key
	t.Run("MissingKey", func(t *testing.T) {
		src.Set(ctx, "verify:missing", "exists", 0)
		dst.Del(ctx, "verify:missing")
		match, err := VerifyKey(ctx, src, dst, "verify:missing")
		if err != nil {
			t.Fatalf("VerifyKey failed: %v", err)
		}
		if match {
			t.Error("Expected match=false when key missing in dest")
		}
	})
}

// TestParseRedisURL tests URL parsing
func TestParseRedisURL(t *testing.T) {
	tests := []struct {
		url      string
		wantDB   int
		wantErr  bool
	}{
		{"redis://127.0.0.1:6379/13", 13, false},
		{"redis://localhost:6379/0", 0, false},
		{"redis://localhost:6379", 0, false},
		{"redis://localhost:6379/", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			client, err := ParseRedisURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRedisURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				// Note: We can't directly access client.Options().DB, so we test indirectly
				client.Close()
			}
		})
	}
}
