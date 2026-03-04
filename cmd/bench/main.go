package main

import (
	"ch-pg-bench/internal/benchmarks"
	"ch-pg-bench/internal/config"
	"ch-pg-bench/internal/models"
	"ch-pg-bench/internal/seed"
	"context"
	"fmt"
	"os"

	"ch-pg-bench/database"
)

func main() {
	if err := run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fmt.Println("ClickHouse vs PostgreSQL")

	// Config load
	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("Error loading config: %w", err)
	}

	ctx := context.Background()

	// Migrations
	err = database.RunMigrations("PostgreSQL", cfg.PostgresDSN, "migrations/postgres")
	if err != nil {
		return fmt.Errorf("Error running PostgreSQL migrations: %w", err)
	}

	err = database.RunMigrations("ClickHouse", cfg.ClickHouseDSN, "migrations/clickhouse")
	if err != nil {
		return fmt.Errorf("Error running ClickHouse migrations: %w", err)
	}

	// ClickHouse connection
	ch, err := database.NewClickHouse(ctx, cfg.ClickHouseDSN)
	if err != nil {
		return fmt.Errorf("Error creating ClickHouse connection: %w", err)
	}
	defer ch.Close()
	fmt.Printf("Connected to ClickHouse: %s\n", cfg.ClickHouseDSN)

	// PostgreSQL connection
	pg, err := database.NewPostgreSQL(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("Error creating PostgreSQL connection: %w", err)
	}
	defer pg.Close()
	fmt.Printf("Connected to PostgreSQL: %s\n", cfg.PostgresDSN)

	// Truncate and seed fresh data
	seeder := seed.NewSeed(pg, cfg.PostgresMainDB, ch, cfg.ClickHouseMainDB)

	if _, err := pg.Exec(ctx, "TRUNCATE TABLE users, trades"); err != nil {
		return fmt.Errorf("Error truncating PG tables: %w", err)
	}
	if err := ch.Exec(ctx, "TRUNCATE TABLE users"); err != nil {
		return fmt.Errorf("Error truncating CH users: %w", err)
	}
	if err := ch.Exec(ctx, "TRUNCATE TABLE trades"); err != nil {
		return fmt.Errorf("Error truncating CH trades: %w", err)
	}

	users, err := seeder.Users(ctx, 10000)
	if err != nil {
		return fmt.Errorf("Error seeding users: %w", err)
	}
	fmt.Printf("Seeded %d users\n", len(users))

	// ******************************** Benchmark 1: Point Lookup ********************************
	var pickedUsersCount int = 200
	var picked []models.User

	for i := range 3 {
		picked = benchmarks.PickRandomUsers(users, pickedUsersCount)

		fmt.Printf("\n--- Point Lookup Benchmark Run #%d ---\n", i+1)
		if err := benchmarks.PointLookupPG(ctx, pg, picked); err != nil {
			return fmt.Errorf("Error running PG point lookup: %w", err)
		}
		if err := benchmarks.PointLookupCH(ctx, ch, picked); err != nil {
			return fmt.Errorf("Error running CH point lookup: %w", err)
		}
		pickedUsersCount *= 5
	}

	// ******************************** Benchmark 2: Single-Row UPDATE ********************************
	var updateCount int = 10

	for i := range 3 {
		picked = benchmarks.PickRandomUsers(users, updateCount)

		fmt.Printf("\n--- Single Update Benchmark Run #%d ---\n", i+1)
		if err := benchmarks.SingleUpdatePG(ctx, pg, picked); err != nil {
			return fmt.Errorf("Error running PG single update: %w", err)
		}
		if err := benchmarks.SingleUpdateCH(ctx, ch, picked); err != nil {
			return fmt.Errorf("Error running CH single update: %w", err)
		}
		updateCount *= 5
	}

	// ******************************** Benchmark 3: UNIQUE Constraint ********************************
	fmt.Printf("\n--- Unique Constraint Benchmark ---\n")
	duplicateEmail := "duplicate_test@example.com"

	if err := benchmarks.UniqueConstraintPG(ctx, pg, duplicateEmail, 20); err != nil {
		return fmt.Errorf("Error running PG unique constraint: %w", err)
	}
	if err := benchmarks.UniqueConstraintCH(ctx, ch, duplicateEmail, 20); err != nil {
		return fmt.Errorf("Error running CH unique constraint: %w", err)
	}

	// ******************************** Benchmark 4: Bulk Insert ********************************
	bulkCounts := []int{100_000, 500_000, 1_000_000}

	for i, count := range bulkCounts {
		fmt.Printf("\n--- Bulk Insert Benchmark Run #%d (%d rows) ---\n", i+1, count)

		trades := benchmarks.GenerateTrades(count)

		// Truncate trades before each run
		pg.Exec(ctx, "TRUNCATE TABLE trades")
		ch.Exec(ctx, "TRUNCATE TABLE trades")

		if err := benchmarks.BulkInsertPG(ctx, pg, trades); err != nil {
			return fmt.Errorf("Error running PG bulk insert: %w", err)
		}
		if err := benchmarks.BulkInsertCH(ctx, ch, trades); err != nil {
			return fmt.Errorf("Error running CH bulk insert: %w", err)
		}
	}

	// ******************************** Benchmark 5: Aggregation ********************************
	fmt.Printf("\n--- Aggregation Benchmark (over %d rows) ---\n", bulkCounts[len(bulkCounts)-1])

	if err := benchmarks.AggregationPG(ctx, pg); err != nil {
		return fmt.Errorf("Error running PG aggregation: %w", err)
	}
	if err := benchmarks.AggregationCH(ctx, ch); err != nil {
		return fmt.Errorf("Error running CH aggregation: %w", err)
	}

	return nil
}
