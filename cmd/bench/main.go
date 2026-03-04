package main

import (
	"ch-pg-bench/internal/config"
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

	return nil
}
