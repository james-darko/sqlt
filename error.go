package sqlt

import "fmt"

// SchemaConflictError holds detailed information about a single schema mismatch
// detected during the schema comparison phase of AutoMigrate.
// It identifies the schema element, the type of conflict, the specific property
// involved, and the expected versus actual values.
type SchemaConflictError struct {
	// ElementName is the name of the schema element (e.g., table name, index name)
	// that has a conflict.
	ElementName string
	// ConflictType provides a category for the conflict
	// (e.g., "ColumnTypeMismatch", "MissingColumn", "PrimaryKeyChanged").
	ConflictType string
	// PropertyName describes the specific part of the element that has a conflict
	// (e.g., "Column 'email'.Type", "PrimaryKey Columns").
	PropertyName string
	// ExpectedValue is the string representation of what the schema definition expected.
	ExpectedValue string
	// ActualValue is the string representation of what was found in the database.
	ActualValue string
	// Err is an optional underlying error that might have caused or been related to this conflict.
	Err error
}

// Error implements the error interface, providing a human-readable description of the conflict.
func (e *SchemaConflictError) Error() string {
	return fmt.Sprintf("schema conflict for %s (%s): property '%s', expected '%s', got '%s'",
		e.ElementName, e.ConflictType, e.PropertyName, e.ExpectedValue, e.ActualValue)
}

// Unwrap allows for inspecting the underlying error using errors.Is or errors.As.
func (e *SchemaConflictError) Unwrap() error {
	return e.Err
}

// ErrSchemaConflicts is an error type that aggregates one or more SchemaConflictError instances.
// It is returned by AutoMigrate when unresolvable differences are found between the desired
// schema and the actual database schema, specifically for table structures.
type ErrSchemaConflicts struct {
	// Conflicts is a slice containing the individual schema conflict details.
	Conflicts []SchemaConflictError
}

// Error implements the error interface. If there's only one conflict, it returns the
// error message of that single conflict. Otherwise, it returns a summary message
// indicating the number of conflicts and the message of the first conflict.
func (e *ErrSchemaConflicts) Error() string {
	if len(e.Conflicts) == 0 {
		return "no schema conflicts" // Should ideally not happen if this error is returned
	}
	if len(e.Conflicts) == 1 {
		return e.Conflicts[0].Error()
	}
	return fmt.Sprintf("%d schema conflicts found; first: %s", len(e.Conflicts), e.Conflicts[0].Error())
}
