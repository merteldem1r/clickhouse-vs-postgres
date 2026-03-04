package benchmarks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func UniqueConstraintPG(ctx context.Context, pgPool *pgxpool.Pool, duplicateEmail string, goroutines int) error {
	fmt.Printf("Unique Constraint PostgreSQL: inserting '%s' from %d concurrent goroutines\n", duplicateEmail, goroutines)

	var successCount atomic.Int32
	var failCount atomic.Int32
	var wg sync.WaitGroup

	start := time.Now()

	for i := range goroutines {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			id := uuid.New()
			_, err := pgPool.Exec(ctx,
				"INSERT INTO users (id, name, email, is_active, created_at) VALUES ($1, $2, $3, $4, $5)",
				id, fmt.Sprintf("duplicate_%d", index), duplicateEmail, true, time.Now(),
			)
			if err != nil {
				failCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("  Completed in %v — Success: %d, Rejected: %d\n", time.Since(start), successCount.Load(), failCount.Load())
	return nil
}

func UniqueConstraintCH(ctx context.Context, chConn clickhouse.Conn, duplicateEmail string, goroutines int) error {
	fmt.Printf("Unique Constraint ClickHouse: inserting '%s' from %d concurrent goroutines\n", duplicateEmail, goroutines)

	var successCount atomic.Int32
	var failCount atomic.Int32
	var wg sync.WaitGroup

	start := time.Now()

	for i := range goroutines {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			id := uuid.New()
			err := chConn.Exec(ctx,
				"INSERT INTO users (id, name, email, is_active, created_at) VALUES (?, ?, ?, ?, ?)",
				id, fmt.Sprintf("duplicate_%d", index), duplicateEmail, uint8(1), time.Now(),
			)
			if err != nil {
				failCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("  Completed in %v — Success: %d, Rejected: %d\n", time.Since(start), successCount.Load(), failCount.Load())

	// Show the duplicates sitting in ClickHouse
	var dupCount uint64
	err := chConn.QueryRow(ctx,
		"SELECT count(*) FROM users WHERE email = ?", duplicateEmail,
	).Scan(&dupCount)
	if err != nil {
		return fmt.Errorf("ch unique check: %w", err)
	}
	fmt.Printf("  ClickHouse rows with email '%s': %d (all duplicates accepted)\n", duplicateEmail, dupCount)

	return nil
}
