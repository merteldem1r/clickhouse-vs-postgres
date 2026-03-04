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
	cfg, err := config.LoadConfig()

	if err != nil {
		return fmt.Errorf("Error loading config: %w", err)
	}

	ctx := context.Background()

	// ClickHouse connection
	ch, err := database.NewClickHouse(ctx, cfg.ClickHouseDSN)
	if err != nil {
		return fmt.Errorf("Error creating ClickHouse connection: %w", err)
	}
	defer ch.Close()

	// PostgreSQL connection
	pg, err := database.NewPostgreSQL(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("Error creating PostgreSQL connection: %w", err)
	}
	defer pg.Close()

	fmt.Println("ClickHouse & PostgreSQL")

	return nil
}
