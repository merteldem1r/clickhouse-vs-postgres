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

	// Seeding data
	seeder := seed.NewSeed(pg, cfg.PostgresMainDB, ch, cfg.ClickHouseMainDB)

	// Seed 10k users or load existing
	var count int
	pg.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&count)

	var users []models.User
	if count > 0 {
		fmt.Printf("Users already seeded: %d\n", count)
		users, err = seed.LoadUsers(ctx, pg)
		if err != nil {
			return fmt.Errorf("Error loading users: %w", err)
		}
	} else {
		users, err = seeder.Users(ctx, 10000)
		if err != nil {
			return fmt.Errorf("Error seeding users: %w", err)
		}
		fmt.Printf("Seeded %d users\n", len(users))
	}

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

	return nil
}
