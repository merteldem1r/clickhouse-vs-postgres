package benchmarks

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

func AggregationPG(ctx context.Context, pgPool *pgxpool.Pool) error {
	fmt.Println("  PostgreSQL:")

	// Query 1: Count per symbol in last 24 hours
	start := time.Now()
	rows, err := pgPool.Query(ctx,
		"SELECT symbol, count(*) FROM trades WHERE timestamp >= $1 GROUP BY symbol ORDER BY count(*) DESC",
		time.Now().Add(-24*time.Hour),
	)
	if err != nil {
		return fmt.Errorf("pg agg query 1: %w", err)
	}
	rows.Close()
	fmt.Printf("    Count per symbol:       %v\n", time.Since(start))

	// Query 2: Average price per minute (time bucketing)
	start = time.Now()
	rows, err = pgPool.Query(ctx,
		"SELECT date_trunc('minute', timestamp) as minute, avg(price) FROM trades GROUP BY minute ORDER BY minute LIMIT 100",
	)
	if err != nil {
		return fmt.Errorf("pg agg query 2: %w", err)
	}
	rows.Close()
	fmt.Printf("    Avg price per minute:   %v\n", time.Since(start))

	// Query 3: Top 10 symbols by total volume
	start = time.Now()
	rows, err = pgPool.Query(ctx,
		"SELECT symbol, sum(volume) as total_vol FROM trades GROUP BY symbol ORDER BY total_vol DESC LIMIT 10",
	)
	if err != nil {
		return fmt.Errorf("pg agg query 3: %w", err)
	}
	rows.Close()
	fmt.Printf("    Top 10 by volume:       %v\n", time.Since(start))

	return nil
}

func AggregationCH(ctx context.Context, chConn clickhouse.Conn) error {
	fmt.Println("  ClickHouse:")

	// Query 1: Count per symbol in last 24 hours
	start := time.Now()
	rows, err := chConn.Query(ctx,
		"SELECT symbol, count(*) FROM trades WHERE timestamp >= ? GROUP BY symbol ORDER BY count(*) DESC",
		time.Now().Add(-24*time.Hour),
	)
	if err != nil {
		return fmt.Errorf("ch agg query 1: %w", err)
	}
	rows.Close()
	fmt.Printf("    Count per symbol:       %v\n", time.Since(start))

	// Query 2: Average price per minute (time bucketing)
	start = time.Now()
	rows, err = chConn.Query(ctx,
		"SELECT toStartOfMinute(timestamp) as minute, avg(price) FROM trades GROUP BY minute ORDER BY minute LIMIT 100",
	)
	if err != nil {
		return fmt.Errorf("ch agg query 2: %w", err)
	}
	rows.Close()
	fmt.Printf("    Avg price per minute:   %v\n", time.Since(start))

	// Query 3: Top 10 symbols by total volume
	start = time.Now()
	rows, err = chConn.Query(ctx,
		"SELECT symbol, sum(volume) as total_vol FROM trades GROUP BY symbol ORDER BY total_vol DESC LIMIT 10",
	)
	if err != nil {
		return fmt.Errorf("ch agg query 3: %w", err)
	}
	rows.Close()
	fmt.Printf("    Top 10 by volume:       %v\n", time.Since(start))

	return nil
}
