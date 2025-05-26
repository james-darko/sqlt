package sqlt

// SchemaDefinition holds all parsed elements from a database schema,
// categorizing them into tables, indexes, views, and triggers.
// It serves as a comprehensive representation of a database's structure.
type SchemaDefinition struct {
	// Tables is a map of table names to their TableDefinition.
	Tables map[string]TableDefinition
	// Indexes is a map of index names to their IndexDefinition.
	Indexes map[string]IndexDefinition
	// Views is a map of view names to their ViewDefinition.
	Views map[string]ViewDefinition
	// Triggers is a map of trigger names to their TriggerDefinition.
	Triggers map[string]TriggerDefinition
}

// TableDefinition represents a table in the database schema.
// It includes the table's name, its full SQL definition, column details,
// primary key information, and any unique constraints.
type TableDefinition struct {
	// Name is the name of the table.
	Name string
	// SQL is the full 'CREATE TABLE' statement used to define the table.
	// This should be the canonical SQL representation.
	SQL string
	// Columns is a slice of ColumnDefinition, detailing each column in the table.
	Columns []ColumnDefinition
	// PrimaryKey is a slice of column names that form the table's primary key.
	// For composite primary keys, this slice will contain multiple names.
	PrimaryKey []string
	// UniqueConstraints is a map where keys are constraint names (if explicitly named)
	// or auto-generated names, and values are slices of column names
	// that form a unique constraint.
	UniqueConstraints map[string][]string
}

// ColumnDefinition represents a single column within a table.
// It captures the column's name, data type, nullability, default value,
// and whether it's part of a primary key, unique constraint, or foreign key.
type ColumnDefinition struct {
	// Name is the name of the column.
	Name string
	// Type is the data type of the column (e.g., "INTEGER", "TEXT", "VARCHAR(255)").
	Type string
	// IsNullable indicates whether the column can store NULL values.
	IsNullable bool
	// DefaultValue is a pointer to a string representing the column's default value.
	// A nil pointer indicates that the column has no explicit default value.
	// An empty string might represent `DEFAULT ''`.
	DefaultValue *string
	// IsPrimaryKey indicates if this column is part of the table's primary key.
	// This can be true even if the primary key is composite.
	IsPrimaryKey bool
	// IsUnique indicates if this column is part of a unique constraint.
	// This can be true for a column-level unique constraint or if the column
	// is part of a table-level unique constraint.
	IsUnique bool
	// ForeignKey holds the definition of the foreign key constraint if this column
	// is part of one. It's nil if the column is not part of a foreign key.
	ForeignKey *ForeignKeyDefinition
}

// ForeignKeyDefinition represents a foreign key constraint on a column or set of columns.
// It specifies the target table and columns, as well as actions for ON UPDATE and ON DELETE.
type ForeignKeyDefinition struct {
	// TargetTable is the name of the table that this foreign key references.
	TargetTable string
	// TargetColumns is a slice of column names in the target table that this
	// foreign key references.
	TargetColumns []string
	// OnUpdate specifies the action to take when a referenced key is updated
	// (e.g., "CASCADE", "SET NULL", "NO ACTION").
	OnUpdate string
	// OnDelete specifies the action to take when a referenced key is deleted
	// (e.g., "CASCADE", "SET NULL", "NO ACTION").
	OnDelete string
}

// IndexDefinition represents an index in the database schema.
// It includes the index's name, the table it belongs to, its full SQL definition,
// the columns it covers, and whether it's a unique index.
type IndexDefinition struct {
	// Name is the name of the index.
	Name string
	// TableName is the name of the table on which the index is created.
	TableName string
	// SQL is the full 'CREATE INDEX' statement used to define the index.
	// This should be the canonical SQL representation.
	SQL string
	// Columns is a slice of column names that the index covers.
	// The order of columns can be significant for multi-column indexes.
	Columns []string
	// IsUnique indicates whether the index enforces unique values.
	IsUnique bool
}

// ViewDefinition represents a view in the database schema.
// It includes the view's name and its full SQL definition.
type ViewDefinition struct {
	// Name is the name of the view.
	Name string
	// SQL is the full 'CREATE VIEW' statement used to define the view.
	// This should be the canonical SQL representation.
	SQL string
}

// TriggerDefinition represents a trigger in the database schema.
// It includes the trigger's name, the table it's associated with,
// and its full SQL definition.
type TriggerDefinition struct {
	// Name is the name of the trigger.
	Name string
	// TableName is the name of the table on which the trigger operates.
	TableName string
	// SQL is the full 'CREATE TRIGGER' statement used to define the trigger.
	// This should be the canonical SQL representation.
	SQL string
}
