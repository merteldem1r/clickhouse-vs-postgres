package benchmarks

import (
	"ch-pg-bench/internal/models"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

// selects random users
func PickRandomUsers(users []models.User, count int) []models.User {
	picked := make([]models.User, 0, count)
	for i := 0; i < count; i++ {
		picked = append(picked, users[rand.Intn(len(users))])
	}
	return picked
}

func PointLookupPG(ctx context.Context, pgPool *pgxpool.Pool, users []models.User) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Point Lookup PostgreSQL: %d queries in %v (avg %v/query)\n",
			len(users), time.Since(start), time.Since(start)/time.Duration(len(users)))
	}()

	for _, user := range users {
		var pgUser models.User
		err := pgPool.QueryRow(ctx, "SELECT id, name, email, is_active, created_at FROM users WHERE id = $1", user.ID).Scan(
			&pgUser.ID, &pgUser.Name, &pgUser.Email, &pgUser.IsActive, &pgUser.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("pg point lookup: %w", err)
		}
	}

	return nil
}

func PointLookupCH(ctx context.Context, chConn clickhouse.Conn, users []models.User) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Point Lookup ClickHouse: %d queries in %v (avg %v/query)\n",
			len(users), time.Since(start), time.Since(start)/time.Duration(len(users)))
	}()

	for _, user := range users {
		var chUser models.User
		var isActive uint8

		err := chConn.QueryRow(ctx, "SELECT id, name, email, is_active, created_at FROM users WHERE id = ?", user.ID).Scan(
			&chUser.ID, &chUser.Name, &chUser.Email, &isActive, &chUser.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("ch point lookup: %w", err)
		}
	}

	return nil
}
