package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type UserFilter struct {
	ID        uuid.UUID       `json:"id"`
	UserID    uuid.UUID       `json:"user_id"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Filters   json.RawMessage `json:"filters"`
}
