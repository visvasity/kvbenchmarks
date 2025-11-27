package kvbenchmarks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/visvasity/kv"
	"github.com/visvasity/kv/kvutil"
)

const valueSize = 1024

var valueBytes = makeValue(valueSize)
var valueReader = bytes.NewReader(valueBytes)

func makeValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i * 41) % 251)
	}
	return b
}

type bench struct {
	name   string
	prefix string // unique key namespace
	fn     func(b *testing.B, db kv.Database, ctx context.Context, prefix string)
}

func RunBenchmarks(ctx context.Context, b *testing.B, db kv.Database) {
	benchmarks := []bench{
		{"WriteRandom", "/WriteRandom/", benchWriteRandom},
		{"ReadRandomHit", "/ReadRandomHit/", benchReadRandomHit},
		{"ReadRandomMiss", "/ReadRandomMiss/", benchReadRandomMiss},
		{"BatchWrite_100", "/BatchWrite_100/", func(b *testing.B, db kv.Database, ctx context.Context, p string) {
			benchBatchWrite(b, db, ctx, p, 100)
		}},
		{"BatchWrite_1000", "/BatchWrite_1000/", func(b *testing.B, db kv.Database, ctx context.Context, p string) {
			benchBatchWrite(b, db, ctx, p, 1000)
		}},
		{"RangeAscend_1000", "/RangeAscend_1000/", func(b *testing.B, db kv.Database, ctx context.Context, p string) {
			benchRangeScan(b, db, ctx, p, 1000, true)
		}},
		{"RangeDescend_1000", "/RangeDescend_1000/", func(b *testing.B, db kv.Database, ctx context.Context, p string) {
			benchRangeScan(b, db, ctx, p, 1000, false)
		}},
		{"SnapshotConsistency", "/SnapshotConsistency/", benchSnapshotConsistency},
		{"TxContention", "/TxContention/", benchTxContention},
	}

	for _, bc := range benchmarks {
		b.Run(bc.name, func(b *testing.B) {
			// Clean before
			cleanupPrefix(b, db, ctx, bc.prefix)

			// Run benchmark
			bc.fn(b, db, ctx, bc.prefix)

			// Clean after
			cleanupPrefix(b, db, ctx, bc.prefix)
		})
	}
}

// ————————————————————————————————————————————————————————————————
// Individual benchmarks (all use prefix)
// ————————————————————————————————————————————————————————————————

func benchWriteRandom(b *testing.B, db kv.Database, ctx context.Context, prefix string) {
	rng := rand.New(rand.NewSource(0xdeadbeef))

	b.ResetTimer()
	defer b.StopTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%skey:%08d", prefix, rng.Intn(1_000_000))
		tx, err := db.NewTransaction(ctx)
		if err != nil || tx.Set(ctx, key, valueReader) != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func benchReadRandomHit(b *testing.B, db kv.Database, ctx context.Context, prefix string) {
	prepopulatePrefix(b, db, ctx, prefix, 100_000)

	rng := rand.New(rand.NewSource(0xdeadbeef))

	b.ResetTimer()
	defer b.StopTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%skey:%08d", prefix, rng.Intn(100_000))
		tx, err := db.NewTransaction(ctx)
		if err != nil {
			b.Fatal(err)
		}
		r, err := tx.Get(ctx, key)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, r)
		_ = tx.Rollback(ctx)
	}
}

func benchReadRandomMiss(b *testing.B, db kv.Database, ctx context.Context, prefix string) {
	rng := rand.New(rand.NewSource(0xdeadbeef))

	b.ResetTimer()
	defer b.StopTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%smissing:%08d", prefix, rng.Intn(1_000_000))
		tx, _ := db.NewTransaction(ctx)
		_, err := tx.Get(ctx, key)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			b.Fatal(err)
		}
		_ = tx.Rollback(ctx)
	}
}

func benchBatchWrite(b *testing.B, db kv.Database, ctx context.Context, prefix string, size int) {
	b.ResetTimer()
	defer b.StopTimer()

	for i := 0; i < b.N; i++ {
		tx, err := db.NewTransaction(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < size; j++ {
			key := fmt.Sprintf("%sitem:%08d:%08d", prefix, i, j)
			if err := tx.Set(ctx, key, valueReader); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func benchRangeScan(b *testing.B, db kv.Database, ctx context.Context, prefix string, limit int, asc bool) {
	prepopulatePrefix(b, db, ctx, prefix, 100_000)

	b.ResetTimer()
	defer b.StopTimer()

	for i := 0; i < b.N; i++ {
		snap, err := db.NewSnapshot(ctx)
		if err != nil {
			b.Fatal(err)
		}

		count := 0
		var it func(context.Context, string, string, *error) iter.Seq2[string, io.Reader]
		if asc {
			it = snap.Ascend
		} else {
			it = snap.Descend
		}

		// Scan only within our prefix
		beg := prefix
		end := prefix + "\xff" // one past last key in prefix

		for _, val := range it(ctx, beg, end, nil) {
			_, _ = io.Copy(io.Discard, val)
			count++
			if count >= limit {
				break
			}
		}
		_ = snap.Discard(ctx)
	}
}

func benchSnapshotConsistency(b *testing.B, db kv.Database, ctx context.Context, prefix string) {
	prepopulatePrefix(b, db, ctx, prefix, 10_000)

	b.ResetTimer()
	defer b.StopTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			if r.Intn(10) < 2 {
				tx, _ := db.NewTransaction(ctx)
				key := fmt.Sprintf("%skey:%08d", prefix, r.Intn(10_000))
				_ = tx.Set(ctx, key, valueReader)
				_ = tx.Commit(ctx)
			} else {
				snap, err := db.NewSnapshot(ctx)
				if err != nil {
					continue
				}
				beg := prefix
				end := prefix + "\xff"
				for _, val := range snap.Ascend(ctx, beg, end, nil) {
					_, _ = io.Copy(io.Discard, val)
				}
				_ = snap.Discard(ctx)
			}
		}
	})
}

func benchTxContention(b *testing.B, db kv.Database, ctx context.Context, prefix string) {
	hotKey := prefix + "hot"
	var conflicts atomic.Int64

	// Initialize the hot key once
	tx, _ := db.NewTransaction(ctx)
	_ = tx.Set(ctx, hotKey, valueReader)
	_ = tx.Commit(ctx)

	b.ResetTimer()
	defer b.StopTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := db.NewTransaction(ctx)
			if err != nil {
				continue
			}
			_, _ = tx.Get(ctx, hotKey)
			_ = tx.Set(ctx, hotKey, valueReader)
			if err := tx.Commit(ctx); err != nil {
				conflicts.Add(1)
			}
		}
	})
	b.Logf("Conflicts: %d", conflicts.Load())
}

// ————————————————————————————————————————————————————————————————
// Helpers
// ————————————————————————————————————————————————————————————————

func prepopulatePrefix(b *testing.B, db kv.Database, ctx context.Context, prefix string, n int) {
	tx, err := db.NewTransaction(ctx)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%skey:%08d", prefix, i)
		if err := tx.Set(ctx, key, valueReader); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
}

func cleanupPrefix(b *testing.B, db kv.Database, ctx context.Context, prefix string) {
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		return // best effort
	}
	defer snap.Discard(ctx)

	beg, end := kvutil.PrefixRange(prefix)

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)

	for key := range snap.Ascend(ctx, beg, end, nil) {
		_ = tx.Delete(ctx, key)
	}
	_ = tx.Commit(ctx)
}
