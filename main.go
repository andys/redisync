package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	from := flag.String("from", "", "Source Redis URL (redis:// or rediss://)")
	to := flag.String("to", "", "Destination Redis URL (redis:// or rediss://)")
	workers := flag.Int("workers", 20, "Number of concurrent workers")
	forceTypeBased := flag.Bool("type-based", false, "Force type-based sync instead of DUMP/RESTORE")
	verify := flag.Bool("verify", false, "Verify mode: compare data instead of copying")
	flag.Parse()

	if *from == "" || *to == "" {
		fmt.Fprintln(os.Stderr, "Usage: redisync -from <url> -to <url>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()

	srcClient, err := ParseRedisURL(*from)
	if err != nil {
		log.Fatalf("Failed to parse source URL: %v", err)
	}
	defer srcClient.Close()

	dstClient, err := ParseRedisURL(*to)
	if err != nil {
		log.Fatalf("Failed to parse destination URL: %v", err)
	}
	defer dstClient.Close()

	opts := SyncOptions{
		Workers:        *workers,
		ForceTypeBased: *forceTypeBased,
		VerifyMode:     *verify,
	}

	result, err := Sync(ctx, srcClient, dstClient, opts)
	if err != nil {
		log.Fatalf("Sync failed: %v", err)
	}

	// Print results
	fmt.Printf("Source Redis: %s, Destination Redis: %s\n", result.SrcVersion, result.DstVersion)

	if !opts.VerifyMode {
		if opts.ForceTypeBased {
			fmt.Println("Using type-based sync (forced via -type-based flag)")
		} else if result.TypeBased {
			fmt.Printf("Warning: Redis major version mismatch, using type-based sync\n")
		}
	} else {
		fmt.Println("Verify mode: comparing data between source and destination")
	}

	if result.TotalKeys == 0 {
		if opts.VerifyMode {
			fmt.Println("No keys to verify")
		} else {
			fmt.Println("No keys to sync")
		}
		return
	}

	if opts.VerifyMode {
		fmt.Printf("Total keys to verify: %d\n", result.TotalKeys)
		fmt.Printf("%d of %d keys verified\n", result.Completed, result.TotalKeys)
		if result.Mismatched == 0 {
			fmt.Println("Verification complete! All keys match.")
		} else {
			fmt.Printf("Verification complete! %d keys mismatched.\n", result.Mismatched)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Total keys to sync: %d\n", result.TotalKeys)
		fmt.Printf("%d of %d keys done\n", result.Completed, result.TotalKeys)
		fmt.Println("Sync complete!")
	}
}
