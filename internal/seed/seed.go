package seed

import (
	"ch-pg-bench/internal/models"
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Seed struct {
	pgPool   *pgxpool.Pool
	pgDBName string
	chConn   clickhouse.Conn
	chDBName string
}

func NewSeed(pgPool *pgxpool.Pool, pgDBName string, chConn clickhouse.Conn, chDBName string) *Seed {
	return &Seed{
		pgPool:   pgPool,
		pgDBName: pgDBName,
		chConn:   chConn,
		chDBName: chDBName,
	}
}

func (s *Seed) Users(ctx context.Context, count int) ([]models.User, error) {
	// generate users
	users := make([]models.User, 0, count)

	for i := 0; i < count; i++ {
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("seed: generate uuid: %w", err)
		}

		users = append(users, models.User{
			ID:        id,
			Name:      fmt.Sprintf("user_%d", i),
			Email:     fmt.Sprintf("user_%d@example.com", i),
			IsActive:  true,
			CreatedAt: time.Now(),
		})
	}

	// Insert into postgres
	pgBatch := &pgx.Batch{}
	for _, u := range users {
		pgBatch.Queue(
			"INSERT INTO users (id, name, email, is_active, created_at) VALUES ($1, $2, $3, $4, $5)",
			u.ID, u.Name, u.Email, u.IsActive, u.CreatedAt,
		)
	}

	pgResults := s.pgPool.SendBatch(ctx, pgBatch)
	if err := pgResults.Close(); err != nil {
		return nil, fmt.Errorf("seed: postgres insert: %w", err)
	}
	fmt.Printf("Seeded %d users into PostgreSQL\n", count)

	// insert into clickhouse
	chBatch, err := s.chConn.PrepareBatch(ctx, "INSERT INTO users (id, name, email, is_active, created_at)")
	if err != nil {
		return nil, fmt.Errorf("seed: clickhouse prepare batch: %w", err)
	}

	for _, u := range users {
		if err := chBatch.Append(u.ID, u.Name, u.Email, boolToUInt8(u.IsActive), u.CreatedAt); err != nil {
			return nil, fmt.Errorf("seed: clickhouse append: %w", err)
		}
	}

	if err := chBatch.Send(); err != nil {
		return nil, fmt.Errorf("seed: clickhouse send batch: %w", err)
	}
	fmt.Printf("Seeded %d users into ClickHouse\n", count)

	return users, nil
}

func LoadUsers(ctx context.Context, pgPool *pgxpool.Pool) ([]models.User, error) {
	rows, err := pgPool.Query(ctx, "SELECT id, name, email, is_active, created_at FROM users")
	if err != nil {
		return nil, fmt.Errorf("seed: load users query: %w", err)
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var u models.User
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.IsActive, &u.CreatedAt); err != nil {
			return nil, fmt.Errorf("seed: load users scan: %w", err)
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("seed: load users iteration: %w", err)
	}

	return users, nil
}

// Go bool -> ClickHouse UInt8 0 / 1
func boolToUInt8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
