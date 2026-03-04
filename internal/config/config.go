package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	PostgresDSN    string `env:"POSTGRES_DSN" env-required:"true"`
	PostgresMainDB string `env:"PG_MAIN_DATABASE" env-required:"true"`

	ClickHouseDSN    string `env:"CLICKHOUSE_DSN" env-required:"true"`
	ClickHouseMainDB string `env:"CH_MAIN_DATABASE" env-required:"true"`
}

func LoadConfig() (*Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(".env", &cfg); err != nil {
		return nil, fmt.Errorf("Config error: %w", err)
	}

	return &cfg, nil
}
