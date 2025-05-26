package sqlt

import "fmt" // Added import for fmt

type Error struct {
	err error
}

func (e Error) Error() string {
	return e.err.Error()
}

func (e Error) Unwrap() error {
	return e.err
}

func (e Error) Is(target error) bool {
	_, ok := target.(Error)
	return ok
}

func (e Error) As(target any) bool {
	if t, ok := target.(*Error); ok {
		*t = e
		return true
	}
	return false
}

// SchemaConflictError represents an error due to a schema conflict.
type SchemaConflictError struct {
	ObjectName      string
	ObjectType      string
	ExpectedSQL     string
	ActualSQL       string
	ConflictDetails string
}

// Error returns a formatted error message summarizing the schema conflict.
func (e *SchemaConflictError) Error() string {
	return "schema conflict: " + e.ConflictDetails
}

// ErrTableDeletionNotAllowed represents an error when table deletions are disallowed
// but the schema migration would result in table deletions.
type ErrTableDeletionNotAllowed struct {
	Tables []string
}

// Error returns a formatted error message summarizing the disallowed table deletions.
func (e ErrTableDeletionNotAllowed) Error() string {
	// Use fmt.Sprintf from the fmt package (ensure it's imported in the file if not already)
	return fmt.Sprintf("table deletion not allowed, but the following tables would be deleted: %v", e.Tables)
}
