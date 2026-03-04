package seed

import (
	"ch-pg-bench/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
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

var (
	filterCategories = []string{"electronics", "books", "clothing", "sports", "home", "toys", "food", "beauty"}
	filterBrands     = []string{"apple", "samsung", "sony", "lg", "nike", "adidas", "ikea", "bosch"}
	filterSortOpts   = []string{"price_asc", "price_desc", "rating_desc", "newest", "popular"}
	filterFrequency  = []string{"daily", "weekly", "monthly", "never"}
)

func generateRandomFilter() map[string]interface{} {
	// Pick 2-4 random categories
	catCount := 2 + rand.Intn(3)
	cats := make([]string, 0, catCount)
	perm := rand.Perm(len(filterCategories))
	for i := 0; i < catCount; i++ {
		cats = append(cats, filterCategories[perm[i]])
	}

	// Pick 1-3 random brands
	brandCount := 1 + rand.Intn(3)
	brands := make([]string, 0, brandCount)
	perm = rand.Perm(len(filterBrands))
	for i := 0; i < brandCount; i++ {
		brands = append(brands, filterBrands[perm[i]])
	}

	minPrice := float64(10 + rand.Intn(200))

	return map[string]interface{}{
		"price_range": map[string]interface{}{
			"min": minPrice,
			"max": minPrice + float64(100+rand.Intn(900)),
		},
		"categories": cats,
		"brands":     brands,
		"rating_min": float64(1+rand.Intn(5)) * 0.5,
		"in_stock":   rand.Intn(2) == 1,
		"sort_by":    filterSortOpts[rand.Intn(len(filterSortOpts))],
		"notifications": map[string]interface{}{
			"email":     rand.Intn(2) == 1,
			"push":      rand.Intn(2) == 1,
			"frequency": filterFrequency[rand.Intn(len(filterFrequency))],
		},
	}
}

func (s *Seed) UserFilters(ctx context.Context, users []models.User) ([]models.UserFilter, error) {
	filters := make([]models.UserFilter, 0, len(users))

	for _, u := range users {
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("seed: generate uuid: %w", err)
		}

		filterData := generateRandomFilter()
		jsonBytes, err := json.Marshal(filterData)
		if err != nil {
			return nil, fmt.Errorf("seed: marshal filter: %w", err)
		}

		filters = append(filters, models.UserFilter{
			ID:        id,
			UserID:    u.ID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Filters:   jsonBytes,
		})
	}

	// Insert into PostgreSQL
	pgBatch := &pgx.Batch{}
	for _, f := range filters {
		pgBatch.Queue(
			"INSERT INTO user_filters (id, user_id, created_at, updated_at, filters) VALUES ($1, $2, $3, $4, $5)",
			f.ID, f.UserID, f.CreatedAt, f.UpdatedAt, f.Filters,
		)
	}

	pgResults := s.pgPool.SendBatch(ctx, pgBatch)
	if err := pgResults.Close(); err != nil {
		return nil, fmt.Errorf("seed: postgres insert user_filters: %w", err)
	}
	fmt.Printf("Seeded %d user_filters into PostgreSQL\n", len(filters))

	// Insert into ClickHouse
	chBatch, err := s.chConn.PrepareBatch(ctx, "INSERT INTO user_filters (id, user_id, created_at, updated_at, filters)")
	if err != nil {
		return nil, fmt.Errorf("seed: clickhouse prepare user_filters batch: %w", err)
	}

	for _, f := range filters {
		if err := chBatch.Append(f.ID, f.UserID, f.CreatedAt, f.UpdatedAt, string(f.Filters)); err != nil {
			return nil, fmt.Errorf("seed: clickhouse append user_filter: %w", err)
		}
	}

	if err := chBatch.Send(); err != nil {
		return nil, fmt.Errorf("seed: clickhouse send user_filters batch: %w", err)
	}
	fmt.Printf("Seeded %d user_filters into ClickHouse\n", len(filters))

	return filters, nil
}

// Go bool -> ClickHouse UInt8 0 / 1
func boolToUInt8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
