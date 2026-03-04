package benchmarks

import (
	"ch-pg-bench/internal/models"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var symbols = []string{
	"THYAO", "GARAN", "AKBNK", "EREGL", "BIMAS",
	"SISE", "KCHOL", "TUPRS", "SAHOL", "TCELL",
}

// GenerateTrades creates N random trade rows spread over the last 24 hours
func GenerateTrades(count int) []models.Trade {
	trades := make([]models.Trade, 0, count)
	now := time.Now()
	dayAgo := now.Add(-24 * time.Hour)

	for i := range count {
		_ = i
		ts := dayAgo.Add(time.Duration(rand.Int63n(int64(24 * time.Hour))))

		trades = append(trades, models.Trade{
			ID:        uuid.New(),
			Symbol:    symbols[rand.Intn(len(symbols))],
			Side:      [2]string{"buy", "sell"}[rand.Intn(2)],
			Price:     50.0 + rand.Float64()*450.0, // 50 - 500 TRY
			Volume:    int64(1 + rand.Intn(10000)),
			Timestamp: ts,
		})
	}
	return trades
}

func BulkInsertPG(ctx context.Context, pgPool *pgxpool.Pool, trades []models.Trade) error {
	start := time.Now()

	// Insert in chunks of 5000 to avoid huge batch memory
	chunkSize := 5000
	for i := 0; i < len(trades); i += chunkSize {
		end := i + chunkSize
		if end > len(trades) {
			end = len(trades)
		}

		batch := &pgx.Batch{}
		for _, t := range trades[i:end] {
			batch.Queue(
				"INSERT INTO trades (id, symbol, side, price, volume, timestamp) VALUES ($1, $2, $3, $4, $5, $6)",
				t.ID, t.Symbol, t.Side, t.Price, t.Volume, t.Timestamp,
			)
		}

		results := pgPool.SendBatch(ctx, batch)
		if err := results.Close(); err != nil {
			return fmt.Errorf("pg bulk insert chunk %d: %w", i/chunkSize, err)
		}
	}

	elapsed := time.Since(start)
	rowsPerSec := float64(len(trades)) / elapsed.Seconds()
	fmt.Printf("Bulk Insert PostgreSQL: %d rows in %v (%.0f rows/sec)\n",
		len(trades), elapsed, rowsPerSec)
	return nil
}

func BulkInsertCH(ctx context.Context, chConn clickhouse.Conn, trades []models.Trade) error {
	start := time.Now()

	batch, err := chConn.PrepareBatch(ctx, "INSERT INTO trades (id, symbol, side, price, volume, timestamp)")
	if err != nil {
		return fmt.Errorf("ch bulk insert prepare: %w", err)
	}

	for _, t := range trades {
		if err := batch.Append(t.ID, t.Symbol, t.Side, t.Price, t.Volume, t.Timestamp); err != nil {
			return fmt.Errorf("ch bulk insert append: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("ch bulk insert send: %w", err)
	}

	elapsed := time.Since(start)
	rowsPerSec := float64(len(trades)) / elapsed.Seconds()
	fmt.Printf("Bulk Insert ClickHouse: %d rows in %v (%.0f rows/sec)\n",
		len(trades), elapsed, rowsPerSec)
	return nil
}
