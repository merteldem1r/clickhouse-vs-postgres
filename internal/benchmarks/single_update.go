package benchmarks

import (
	"ch-pg-bench/internal/models"
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

func SingleUpdatePG(ctx context.Context, pgPool *pgxpool.Pool, users []models.User) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Single Update PostgreSQL: %d updates in %v (avg %v/update)\n",
			len(users), time.Since(start), time.Since(start)/time.Duration(len(users)))
	}()

	for _, user := range users {
		_, err := pgPool.Exec(ctx,
			"UPDATE users SET is_active = $1 WHERE id = $2",
			!user.IsActive, user.ID,
		)
		if err != nil {
			return fmt.Errorf("pg single update: %w", err)
		}
	}

	return nil
}

func SingleUpdateCH(ctx context.Context, chConn clickhouse.Conn, users []models.User) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Single Update ClickHouse: %d updates in %v (avg %v/update)\n",
			len(users), time.Since(start), time.Since(start)/time.Duration(len(users)))
	}()

	for _, user := range users {
		newVal := uint8(1)
		if user.IsActive {
			newVal = 0
		}

		err := chConn.Exec(ctx,
			"ALTER TABLE users UPDATE is_active = ? WHERE id = ? SETTINGS mutations_sync = 1",
			newVal, user.ID,
		)
		if err != nil {
			return fmt.Errorf("ch single update: %w", err)
		}
	}

	return nil
}
