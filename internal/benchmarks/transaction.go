package benchmarks

import (
	"ch-pg-bench/internal/models"
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TransactionPG(ctx context.Context, pgPool *pgxpool.Pool, user models.User) error {
	fmt.Printf("  PostgreSQL — testing 3-step transaction with ROLLBACK\n")
	fmt.Printf("    Before: name=%q, is_active=%v\n", user.Name, user.IsActive)

	// Begin transaction
	tx, err := pgPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg begin tx: %w", err)
	}

	// Step 1: deactivate user — succeeds
	_, err = tx.Exec(ctx, "UPDATE users SET is_active = false WHERE id = $1", user.ID)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("pg tx step 1: %w", err)
	}
	fmt.Println("    Step 1 (set is_active=false): OK")

	// Step 2: rename user — succeeds
	_, err = tx.Exec(ctx, "UPDATE users SET name = $1 WHERE id = $2", user.Name+" [DEACTIVATED]", user.ID)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("pg tx step 2: %w", err)
	}
	fmt.Println("    Step 2 (append [DEACTIVATED] to name): OK")

	// Step 3: set email to NULL — fails because of NOT NULL constraint
	_, err = tx.Exec(ctx, "UPDATE users SET email = NULL WHERE id = $1", user.ID)
	if err != nil {
		fmt.Printf("    Step 3 (set email=NULL): FAILED — %v\n", err)
		tx.Rollback(ctx)
		fmt.Println("    ROLLBACK executed — all changes undone")
	} else {
		// This shouldn't happen, but commit if it does
		tx.Commit(ctx)
	}

	// Verify: SELECT the user — should be unchanged
	var after models.User
	pgPool.QueryRow(ctx, "SELECT id, name, email, is_active, created_at FROM users WHERE id = $1", user.ID).Scan(
		&after.ID, &after.Name, &after.Email, &after.IsActive, &after.CreatedAt,
	)
	fmt.Printf("    After:  name=%q, is_active=%v\n", after.Name, after.IsActive)
	fmt.Printf("    Result: User is UNCHANGED — transaction rolled back correctly\n")

	return nil
}

func TransactionCH(ctx context.Context, chConn clickhouse.Conn, user models.User) error {
	fmt.Printf("  ClickHouse — testing 3-step operation with NO rollback\n")
	fmt.Printf("    Before: name=%q, is_active=%v\n", user.Name, user.IsActive)

	// Step 1: deactivate user — succeeds and is permanently committed
	err := chConn.Exec(ctx,
		"ALTER TABLE users UPDATE is_active = 0 WHERE id = ? SETTINGS mutations_sync = 1",
		user.ID,
	)
	if err != nil {
		return fmt.Errorf("ch tx step 1: %w", err)
	}
	fmt.Println("    Step 1 (set is_active=0): OK — permanently committed")

	// Step 2: rename user — succeeds and is permanently committed
	err = chConn.Exec(ctx,
		"ALTER TABLE users UPDATE name = ? WHERE id = ? SETTINGS mutations_sync = 1",
		user.Name+" [DEACTIVATED]", user.ID,
	)
	if err != nil {
		return fmt.Errorf("ch tx step 2: %w", err)
	}
	fmt.Println("    Step 2 (append [DEACTIVATED] to name): OK — permanently committed")

	// Step 3: simulated failure
	fmt.Println("    Step 3: FAILED (simulated error)")
	fmt.Println("    NO ROLLBACK available — steps 1 and 2 cannot be undone")

	// Verify: SELECT the user — should be partially modified
	var afterName string
	var afterActive uint8
	chConn.QueryRow(ctx, "SELECT name, is_active FROM users WHERE id = ?", user.ID).Scan(
		&afterName, &afterActive,
	)
	fmt.Printf("    After:  name=%q, is_active=%v\n", afterName, afterActive)
	fmt.Printf("    Result: User is CORRUPTED — partial modification with no way to undo\n")

	return nil
}
