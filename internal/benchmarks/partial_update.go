package benchmarks

import (
	"ch-pg-bench/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

func PickRandomFilters(filters []models.UserFilter, count int) []models.UserFilter {
	if count >= len(filters) {
		return filters
	}
	perm := rand.Perm(len(filters))
	picked := make([]models.UserFilter, count)
	for i := range count {
		picked[i] = filters[perm[i]]
	}
	return picked
}

// PartialUpdatePG runs three partial JSON update scenarios on PostgreSQL using jsonb_set.
func PartialUpdatePG(ctx context.Context, pgPool *pgxpool.Pool, filters []models.UserFilter) error {
	// Scenario A: Scalar path update — change sort_by
	start := time.Now()
	for _, f := range filters {
		_, err := pgPool.Exec(ctx,
			`UPDATE user_filters
			 SET filters = jsonb_set(filters, '{sort_by}', '"rating_desc"'),
			     updated_at = now()
			 WHERE id = $1`, f.ID)
		if err != nil {
			return fmt.Errorf("pg partial update (scalar): %w", err)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("  Partial Update (scalar)  PostgreSQL: %d updates in %v (avg %v/update)\n",
		len(filters), elapsed, elapsed/time.Duration(len(filters)))

	// Scenario B: Nested path update — change price_range.max
	start = time.Now()
	for _, f := range filters {
		_, err := pgPool.Exec(ctx,
			`UPDATE user_filters
			 SET filters = jsonb_set(filters, '{price_range,max}', '1000'),
			     updated_at = now()
			 WHERE id = $1`, f.ID)
		if err != nil {
			return fmt.Errorf("pg partial update (nested): %w", err)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("  Partial Update (nested)  PostgreSQL: %d updates in %v (avg %v/update)\n",
		len(filters), elapsed, elapsed/time.Duration(len(filters)))

	// Scenario C: Array append — add "sony" to brands
	start = time.Now()
	for _, f := range filters {
		_, err := pgPool.Exec(ctx,
			`UPDATE user_filters
			 SET filters = jsonb_set(filters, '{brands}', (filters->'brands') || '"sony"'::jsonb),
			     updated_at = now()
			 WHERE id = $1`, f.ID)
		if err != nil {
			return fmt.Errorf("pg partial update (array append): %w", err)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("  Partial Update (array)   PostgreSQL: %d updates in %v (avg %v/update)\n",
		len(filters), elapsed, elapsed/time.Duration(len(filters)))

	return nil
}

// PartialUpdateCH runs three partial JSON update scenarios on ClickHouse.
// Since ClickHouse has no jsonb_set equivalent, each update must:
//  1. Read the current JSON string from ClickHouse
//  2. Unmarshal, modify, and re-marshal in Go
//  3. Write back the entire document via ALTER TABLE UPDATE
func PartialUpdateCH(ctx context.Context, chConn clickhouse.Conn, filters []models.UserFilter) error {
	// Scenario A: Scalar path update — change sort_by
	start := time.Now()
	for _, f := range filters {
		// Read current value
		var current string
		err := chConn.QueryRow(ctx,
			"SELECT filters FROM user_filters WHERE id = ?", f.ID).Scan(&current)
		if err != nil {
			return fmt.Errorf("ch partial update (scalar) read: %w", err)
		}

		var doc map[string]any
		if err := json.Unmarshal([]byte(current), &doc); err != nil {
			return fmt.Errorf("ch partial update (scalar) unmarshal: %w", err)
		}
		doc["sort_by"] = "rating_desc"
		updated, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("ch partial update (scalar) marshal: %w", err)
		}

		err = chConn.Exec(ctx,
			"ALTER TABLE user_filters UPDATE filters = ?, updated_at = now() WHERE id = ? SETTINGS mutations_sync = 1",
			string(updated), f.ID)
		if err != nil {
			return fmt.Errorf("ch partial update (scalar) write: %w", err)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("  Partial Update (scalar)  ClickHouse: %d updates in %v (avg %v/update)\n",
		len(filters), elapsed, elapsed/time.Duration(len(filters)))

	// Scenario B: Nested path update — change price_range.max
	start = time.Now()
	for _, f := range filters {
		var current string
		err := chConn.QueryRow(ctx,
			"SELECT filters FROM user_filters WHERE id = ?", f.ID).Scan(&current)
		if err != nil {
			return fmt.Errorf("ch partial update (nested) read: %w", err)
		}

		var doc map[string]any
		if err := json.Unmarshal([]byte(current), &doc); err != nil {
			return fmt.Errorf("ch partial update (nested) unmarshal: %w", err)
		}
		if priceRange, ok := doc["price_range"].(map[string]any); ok {
			priceRange["max"] = 1000
		}
		updated, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("ch partial update (nested) marshal: %w", err)
		}

		err = chConn.Exec(ctx,
			"ALTER TABLE user_filters UPDATE filters = ?, updated_at = now() WHERE id = ? SETTINGS mutations_sync = 1",
			string(updated), f.ID)
		if err != nil {
			return fmt.Errorf("ch partial update (nested) write: %w", err)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("  Partial Update (nested)  ClickHouse: %d updates in %v (avg %v/update)\n",
		len(filters), elapsed, elapsed/time.Duration(len(filters)))

	// Scenario C: Array append — add "sony" to brands
	start = time.Now()
	for _, f := range filters {
		var current string
		err := chConn.QueryRow(ctx,
			"SELECT filters FROM user_filters WHERE id = ?", f.ID).Scan(&current)
		if err != nil {
			return fmt.Errorf("ch partial update (array append) read: %w", err)
		}

		var doc map[string]any
		if err := json.Unmarshal([]byte(current), &doc); err != nil {
			return fmt.Errorf("ch partial update (array append) unmarshal: %w", err)
		}
		if brands, ok := doc["brands"].([]any); ok {
			doc["brands"] = append(brands, "sony")
		}
		updated, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("ch partial update (array append) marshal: %w", err)
		}

		err = chConn.Exec(ctx,
			"ALTER TABLE user_filters UPDATE filters = ?, updated_at = now() WHERE id = ? SETTINGS mutations_sync = 1",
			string(updated), f.ID)
		if err != nil {
			return fmt.Errorf("ch partial update (array append) write: %w", err)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("  Partial Update (array)   ClickHouse: %d updates in %v (avg %v/update)\n",
		len(filters), elapsed, elapsed/time.Duration(len(filters)))

	return nil
}
