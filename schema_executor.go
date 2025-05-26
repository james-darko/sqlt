package sqlt

import (
	"fmt"
)

// ExecuteDeletions performs the deletion of schema elements as specified in the
// `toDelete` SchemaDefinition, within the provided transaction `tx`.
// The function adheres to a specific order of operations to respect dependencies:
// 1. Triggers are dropped.
// 2. Views are dropped.
// 3. Indexes are dropped. (Note: SQLite often drops these automatically with tables,
//    but explicit deletion handles other cases and adds clarity.)
// 4. Tables are dropped.
//
// Parameters:
//   - tx: The database transaction (Tx interface) in which to execute DROP statements.
//   - toDelete: A pointer to a SchemaDefinition containing maps of elements
//     (Triggers, Views, Indexes, Tables) that should be deleted.
//     If nil or if a specific element map (e.g., toDelete.Triggers) is nil,
//     those elements are skipped.
//
// Returns:
//   - nil: If all DROP statements execute successfully or if there's nothing to delete.
//   - error: If any DROP statement fails, the function returns an error immediately,
//     and the transaction should ideally be rolled back by the caller. The error
//     message will include context about the failed operation.
func ExecuteDeletions(tx Tx, toDelete *SchemaDefinition) error {
	if toDelete == nil {
		return nil // Nothing to delete
	}

	// Delete Triggers
	if toDelete.Triggers != nil {
		for triggerName := range toDelete.Triggers {
			query := fmt.Sprintf("DROP TRIGGER IF EXISTS %s;", triggerName)
			_, err := tx.Exec(query)
			if err != nil {
				return fmt.Errorf("failed to drop trigger %s: %w", triggerName, err)
			}
		}
	}

	// Delete Views
	if toDelete.Views != nil {
		for viewName := range toDelete.Views {
			query := fmt.Sprintf("DROP VIEW IF EXISTS %s;", viewName)
			_, err := tx.Exec(query)
			if err != nil {
				return fmt.Errorf("failed to drop view %s: %w", viewName, err)
			}
		}
	}

	// Delete Indexes
	// Note: SQLite automatically drops indexes when a table is dropped.
	// However, explicitly dropping them here handles cases where the table is not dropped
	// or for being explicit. "IF EXISTS" prevents errors.
	if toDelete.Indexes != nil {
		for indexName := range toDelete.Indexes {
			query := fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName)
			_, err := tx.Exec(query)
			if err != nil {
				return fmt.Errorf("failed to drop index %s: %w", indexName, err)
			}
		}
	}

	// Delete Tables
	if toDelete.Tables != nil {
		for tableName := range toDelete.Tables {
			query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName)
			_, err := tx.Exec(query)
			if err != nil {
				return fmt.Errorf("failed to drop table %s: %w", tableName, err)
			}
		}
	}

	return nil
}

// ExecuteCreations performs the creation of schema elements as specified in the
// `toCreate` SchemaDefinition, within the provided transaction `tx`.
// The function uses the canonical SQL statements stored in the definition objects
// (e.g., TableDefinition.SQL).
// The order of creation is critical to respect dependencies:
// 1. Tables are created.
// 2. Indexes are created.
// 3. Views are created.
// 4. Triggers are created.
//
// Parameters:
//   - tx: The database transaction (Tx interface) in which to execute CREATE statements.
//   - toCreate: A pointer to a SchemaDefinition containing maps of elements
//     (Tables, Indexes, Views, Triggers) that should be created using their
//     respective SQL definitions. If nil or if a specific element map is nil,
//     those elements are skipped.
//
// Returns:
//   - nil: If all CREATE statements execute successfully or if there's nothing to create.
//   - error: If any CREATE statement fails (e.g., due to invalid SQL, conflicts),
//     or if an SQL definition string is empty, the function returns an error immediately.
//     The transaction should ideally be rolled back by the caller. The error message
//     will include context about the failed operation.
func ExecuteCreations(tx Tx, toCreate *SchemaDefinition) error {
	if toCreate == nil {
		return nil // Nothing to create
	}

	// Create Tables
	if toCreate.Tables != nil {
		for tableName, tableDef := range toCreate.Tables {
			if tableDef.SQL == "" {
				return fmt.Errorf("empty SQL for table %s", tableName)
			}
			_, err := tx.Exec(tableDef.SQL)
			if err != nil {
				return fmt.Errorf("failed to create table %s: %w (SQL: %s)", tableName, err, tableDef.SQL)
			}
		}
	}

	// Create Indexes
	if toCreate.Indexes != nil {
		for indexName, indexDef := range toCreate.Indexes {
			if indexDef.SQL == "" {
				return fmt.Errorf("empty SQL for index %s", indexName)
			}
			_, err := tx.Exec(indexDef.SQL)
			if err != nil {
				return fmt.Errorf("failed to create index %s: %w (SQL: %s)", indexName, err, indexDef.SQL)
			}
		}
	}

	// Create Views
	if toCreate.Views != nil {
		for viewName, viewDef := range toCreate.Views {
			if viewDef.SQL == "" {
				return fmt.Errorf("empty SQL for view %s", viewName)
			}
			_, err := tx.Exec(viewDef.SQL)
			if err != nil {
				return fmt.Errorf("failed to create view %s: %w (SQL: %s)", viewName, err, viewDef.SQL)
			}
		}
	}

	// Create Triggers
	if toCreate.Triggers != nil {
		for triggerName, triggerDef := range toCreate.Triggers {
			if triggerDef.SQL == "" {
				return fmt.Errorf("empty SQL for trigger %s", triggerName)
			}
			_, err := tx.Exec(triggerDef.SQL)
			if err != nil {
				return fmt.Errorf("failed to create trigger %s: %w (SQL: %s)", triggerName, err, triggerDef.SQL)
			}
		}
	}

	return nil
}
