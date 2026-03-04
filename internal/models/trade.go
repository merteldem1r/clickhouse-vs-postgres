package models

import (
	"time"

	"github.com/google/uuid"
)

type Trade struct {
	ID        uuid.UUID `json:"id"`
	Symbol    string    `json:"symbol"`
	Side      string    `json:"side"`
	Price     float64   `json:"price"`
	Volume    int64     `json:"volume"`
	Timestamp time.Time `json:"timestamp"`
}
