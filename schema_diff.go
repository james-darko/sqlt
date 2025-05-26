package sqlt

import "fmt"

// SchemaDifferences categorizes the differences found between two SchemaDefinition objects.
// It serves as an intermediate representation used by AutoMigrate to determine
// which schema elements to create, delete, or compare further.
type SchemaDifferences struct {
	// ToCreate contains schema elements (tables, indexes, views, triggers)
	// that are present in the desired schema but not in the current schema.
	// These elements are candidates for creation.
	ToCreate SchemaDefinition
	// ToDelete contains schema elements that are present in the current schema
	// but not in the desired schema. These elements are candidates for deletion.
	ToDelete SchemaDefinition
	// ToCompareTables maps table names to TableComparison structs for tables
	// that exist in both desired and current schemas. These tables require
	// detailed structural comparison to detect potential conflicts.
	ToCompareTables map[string]TableComparison
	// ToCompareIndexes maps index names to ElementComparison structs for indexes
	// that exist in both schemas. Their SQL definitions will be compared.
	ToCompareIndexes map[string]ElementComparison
	// ToCompareViews maps view names to ElementComparison structs for views
	// that exist in both schemas. Their SQL definitions will be compared.
	ToCompareViews map[string]ElementComparison
	// ToCompareTriggers maps trigger names to ElementComparison structs for triggers
	// that exist in both schemas. Their SQL definitions will be compared.
	ToCompareTriggers map[string]ElementComparison
}

// TableComparison holds the desired and current definitions for a specific table
// that exists in both the desired and current schemas. This struct is used to
// facilitate detailed structural comparison of the table.
type TableComparison struct {
	// Desired is the TableDefinition from the desired schema.
	Desired TableDefinition
	// Current is the TableDefinition from the current (actual) database schema.
	Current TableDefinition
}

// ElementComparison holds the desired and current SQL definitions for a schema element
// (like an index, view, or trigger) that exists in both desired and current schemas.
// This is used for a direct SQL string comparison to detect changes.
type ElementComparison struct {
	// DesiredSQL is the canonical SQL definition of the element from the desired schema.
	DesiredSQL string
	// CurrentSQL is the canonical SQL definition of the element from the current schema.
	CurrentSQL string
}

// InitializeSchemaDifferences creates and returns a new SchemaDifferences struct
// with all its internal maps and SchemaDefinition fields properly initialized.
// This ensures that the struct is ready to be populated without causing nil pointer dereferences.
func InitializeSchemaDifferences() *SchemaDifferences {
	return &SchemaDifferences{
		ToCreate: SchemaDefinition{
			Tables:   make(map[string]TableDefinition),
			Indexes:  make(map[string]IndexDefinition),
			Views:    make(map[string]ViewDefinition),
			Triggers: make(map[string]TriggerDefinition),
		},
		ToDelete: SchemaDefinition{
			Tables:   make(map[string]TableDefinition),
			Indexes:  make(map[string]IndexDefinition),
			Views:    make(map[string]ViewDefinition),
			Triggers: make(map[string]TriggerDefinition),
		},
		ToCompareTables:  make(map[string]TableComparison),
		ToCompareIndexes: make(map[string]ElementComparison),
		ToCompareViews:   make(map[string]ElementComparison),
		ToCompareTriggers: make(map[string]ElementComparison),
	}
}

// IdentifySchemaDifferences compares a desiredSchema with a currentSchema and
// categorizes all differences into a SchemaDifferences struct.
//
// This function performs a high-level comparison:
// - Elements in desiredSchema but not in currentSchema are added to `diffs.ToCreate`.
// - Elements in currentSchema but not in desiredSchema are added to `diffs.ToDelete`.
// - Elements present in both are added to the respective `diffs.ToCompare...` maps
//   for further detailed comparison by other functions (e.g., CompareTableDefinitions
//   or direct SQL string comparison for indexes, views, triggers).
//
// Parameters:
//   - desiredSchema: The target schema definition.
//   - currentSchema: The existing schema definition from the database.
//
// Returns:
//   A pointer to a SchemaDifferences struct containing the categorized differences.
//   Handles nil input schemas by treating them as empty.
func IdentifySchemaDifferences(desiredSchema, currentSchema *SchemaDefinition) *SchemaDifferences {
	diffs := InitializeSchemaDifferences()

	if desiredSchema == nil {
		desiredSchema = &SchemaDefinition{} // Treat nil as empty
	}
	if currentSchema == nil {
		currentSchema = &SchemaDefinition{} // Treat nil as empty
	}
	if desiredSchema.Tables == nil {
		desiredSchema.Tables = make(map[string]TableDefinition)
	}
	if currentSchema.Tables == nil {
		currentSchema.Tables = make(map[string]TableDefinition)
	}
	if desiredSchema.Indexes == nil {
		desiredSchema.Indexes = make(map[string]IndexDefinition)
	}
	if currentSchema.Indexes == nil {
		currentSchema.Indexes = make(map[string]IndexDefinition)
	}
	if desiredSchema.Views == nil {
		desiredSchema.Views = make(map[string]ViewDefinition)
	}
	if currentSchema.Views == nil {
		currentSchema.Views = make(map[string]ViewDefinition)
	}
	if desiredSchema.Triggers == nil {
		desiredSchema.Triggers = make(map[string]TriggerDefinition)
	}
	if currentSchema.Triggers == nil {
		currentSchema.Triggers = make(map[string]TriggerDefinition)
	}


	// Compare Tables
	for name, desiredTable := range desiredSchema.Tables {
		if currentTable, exists := currentSchema.Tables[name]; exists {
			diffs.ToCompareTables[name] = TableComparison{
				Desired: desiredTable,
				Current: currentTable,
			}
		} else {
			diffs.ToCreate.Tables[name] = desiredTable
		}
	}
	for name, currentTable := range currentSchema.Tables {
		if _, exists := desiredSchema.Tables[name]; !exists {
			diffs.ToDelete.Tables[name] = currentTable
		}
	}

	// Compare Indexes
	for name, desiredIndex := range desiredSchema.Indexes {
		if currentIndex, exists := currentSchema.Indexes[name]; exists {
			diffs.ToCompareIndexes[name] = ElementComparison{
				DesiredSQL: desiredIndex.SQL,
				CurrentSQL: currentIndex.SQL,
			}
		} else {
			diffs.ToCreate.Indexes[name] = desiredIndex
		}
	}
	for name, currentIndex := range currentSchema.Indexes {
		if _, exists := desiredSchema.Indexes[name]; !exists {
			diffs.ToDelete.Indexes[name] = currentIndex
		}
	}

	// Compare Views
	for name, desiredView := range desiredSchema.Views {
		if currentView, exists := currentSchema.Views[name]; exists {
			diffs.ToCompareViews[name] = ElementComparison{
				DesiredSQL: desiredView.SQL,
				CurrentSQL: currentView.SQL,
			}
		} else {
			diffs.ToCreate.Views[name] = desiredView
		}
	}
	for name, currentView := range currentSchema.Views {
		if _, exists := desiredSchema.Views[name]; !exists {
			diffs.ToDelete.Views[name] = currentView
		}
	}

	// Compare Triggers
	for name, desiredTrigger := range desiredSchema.Triggers {
		if currentTrigger, exists := currentSchema.Triggers[name]; exists {
			diffs.ToCompareTriggers[name] = ElementComparison{
				DesiredSQL: desiredTrigger.SQL,
				CurrentSQL: currentTrigger.SQL,
			}
		} else {
			diffs.ToCreate.Triggers[name] = desiredTrigger
		}
	}
	for name, currentTrigger := range currentSchema.Triggers {
		if _, exists := desiredSchema.Triggers[name]; !exists {
			diffs.ToDelete.Triggers[name] = currentTrigger
		}
	}

	return diffs
}

// CompareTableDefinitions performs a detailed structural comparison of two TableDefinition objects.
// It checks for differences in column count, column definitions (name, type, nullability,
// default value, primary key status, foreign key details), table-level primary keys,
// and table-level unique constraints.
//
// This function assumes that prior normalization (e.g., sorting of columns or
// primary key/unique constraint column lists) has been performed on the input
// TableDefinition objects if such order-agnostic comparison is required.
//
// Parameters:
//   - desired: The TableDefinition from the desired schema.
//   - current: The TableDefinition from the current (actual) database schema.
//
// Returns:
//   - *SchemaConflictError: A pointer to a SchemaConflictError detailing the first
//     significant structural difference found. The error includes the table name,
//     conflict type, property name, and expected/actual values.
//   - nil: If no structural differences are found between the two table definitions,
//     indicating they are structurally equivalent. Cosmetic differences in the
//     original SQL (like comments or whitespace) might exist if the SQL strings
//     differ but this function returns nil.
func CompareTableDefinitions(desired, current TableDefinition) *SchemaConflictError {
	// This is a basic check. If SQL is identical, structures are assumed identical.
	// The rqlite parser normalizes SQL, so this should be fairly reliable.
	// Note: SQL comparison is now done in verifySchema and ProcessSchemaDifferences for non-table elements.
	// For tables, we do a structural comparison first. If structures are identical, SQL diff may not matter for AutoMigrate.

	if len(desired.Columns) != len(current.Columns) {
		return &SchemaConflictError{
			ElementName:   desired.Name,
			ConflictType:  "ColumnCountMismatch",
			PropertyName:  "Table Columns",
			ExpectedValue: fmt.Sprintf("%d", len(desired.Columns)),
			ActualValue:   fmt.Sprintf("%d", len(current.Columns)),
		}
	}

	// Assuming columns are sorted by name by normalizeSchemaDefinition before this call,
	// or we sort them here if not guaranteed. For now, assume they are in comparable order.
	for i := range desired.Columns {
		dCol := desired.Columns[i]
		cCol := current.Columns[i]

		if dCol.Name != cCol.Name {
			return &SchemaConflictError{
				ElementName:   desired.Name,
				ConflictType:  "ColumnNameMismatch",
				PropertyName:  fmt.Sprintf("Column at index %d Name", i),
				ExpectedValue: dCol.Name,
				ActualValue:   cCol.Name,
			}
		}
		if dCol.Type != cCol.Type {
			return &SchemaConflictError{
				ElementName:   desired.Name,
				ConflictType:  "ColumnTypeMismatch",
				PropertyName:  fmt.Sprintf("Column '%s'.Type", dCol.Name),
				ExpectedValue: dCol.Type,
				ActualValue:   cCol.Type,
			}
		}
		if dCol.IsNullable != cCol.IsNullable {
			return &SchemaConflictError{
				ElementName:   desired.Name,
				ConflictType:  "ColumnNullabilityMismatch",
				PropertyName:  fmt.Sprintf("Column '%s'.IsNullable", dCol.Name),
				ExpectedValue: fmt.Sprintf("%t", dCol.IsNullable),
				ActualValue:   fmt.Sprintf("%t", cCol.IsNullable),
			}
		}

		dDefault := "NULL"
		if dCol.DefaultValue != nil {
			dDefault = *dCol.DefaultValue
		}
		cDefault := "NULL"
		if cCol.DefaultValue != nil {
			cDefault = *cCol.DefaultValue
		}
		if (dCol.DefaultValue == nil && cCol.DefaultValue != nil) || (dCol.DefaultValue != nil && cCol.DefaultValue == nil) ||
			(dCol.DefaultValue != nil && cCol.DefaultValue != nil && *dCol.DefaultValue != *cCol.DefaultValue) {
			return &SchemaConflictError{
				ElementName:   desired.Name,
				ConflictType:  "ColumnDefaultValueMismatch",
				PropertyName:  fmt.Sprintf("Column '%s'.DefaultValue", dCol.Name),
				ExpectedValue: dDefault,
				ActualValue:   cDefault,
			}
		}

		if dCol.IsPrimaryKey != cCol.IsPrimaryKey {
			return &SchemaConflictError{
				ElementName:   desired.Name,
				ConflictType:  "ColumnPrimaryKeyMismatch",
				PropertyName:  fmt.Sprintf("Column '%s'.IsPrimaryKey", dCol.Name),
				ExpectedValue: fmt.Sprintf("%t", dCol.IsPrimaryKey),
				ActualValue:   fmt.Sprintf("%t", cCol.IsPrimaryKey),
			}
		}
		// IsUnique can be complex due to table-level constraints.
		// This check is for column-level unique.
		// If IsUnique is derived from all unique constraints, this check might be too simple.
		// For now, a direct comparison is made.
		if dCol.IsUnique != cCol.IsUnique {
			// This might be too noisy if IsUnique is true because of a table-level constraint
			// that is handled elsewhere. However, if it's a column-def specific UNIQUE, it's a mismatch.
			// Let's assume IsUnique is set correctly by the parser for column-level UNIQUE.
		}


		// Compare ForeignKey
		if (dCol.ForeignKey == nil && cCol.ForeignKey != nil) || (dCol.ForeignKey != nil && cCol.ForeignKey == nil) {
			return &SchemaConflictError{
				ElementName:   desired.Name,
				ConflictType:  "ForeignKeyPresenceMismatch",
				PropertyName:  fmt.Sprintf("Column '%s'.ForeignKey", dCol.Name),
				ExpectedValue: fmt.Sprintf("%t", dCol.ForeignKey != nil),
				ActualValue:   fmt.Sprintf("%t", cCol.ForeignKey != nil),
			}
		}
		if dCol.ForeignKey != nil && cCol.ForeignKey != nil {
			if dCol.ForeignKey.TargetTable != cCol.ForeignKey.TargetTable {
				return &SchemaConflictError{ElementName: desired.Name, ConflictType: "ForeignKeyMismatch", PropertyName: fmt.Sprintf("Column '%s'.ForeignKey.TargetTable", dCol.Name), ExpectedValue: dCol.ForeignKey.TargetTable, ActualValue: cCol.ForeignKey.TargetTable}
			}
			if !compareStringSlicesEquivalent(dCol.ForeignKey.TargetColumns, cCol.ForeignKey.TargetColumns) { // Order might not matter
				return &SchemaConflictError{ElementName: desired.Name, ConflictType: "ForeignKeyMismatch", PropertyName: fmt.Sprintf("Column '%s'.ForeignKey.TargetColumns", dCol.Name), ExpectedValue: fmt.Sprint(dCol.ForeignKey.TargetColumns), ActualValue: fmt.Sprint(cCol.ForeignKey.TargetColumns)}
			}
			if dCol.ForeignKey.OnUpdate != cCol.ForeignKey.OnUpdate {
				return &SchemaConflictError{ElementName: desired.Name, ConflictType: "ForeignKeyMismatch", PropertyName: fmt.Sprintf("Column '%s'.ForeignKey.OnUpdate", dCol.Name), ExpectedValue: dCol.ForeignKey.OnUpdate, ActualValue: cCol.ForeignKey.OnUpdate}
			}
			if dCol.ForeignKey.OnDelete != cCol.ForeignKey.OnDelete {
				return &SchemaConflictError{ElementName: desired.Name, ConflictType: "ForeignKeyMismatch", PropertyName: fmt.Sprintf("Column '%s'.ForeignKey.OnDelete", dCol.Name), ExpectedValue: dCol.ForeignKey.OnDelete, ActualValue: cCol.ForeignKey.OnDelete}
			}
		}
	}

	// Compare table-level PrimaryKey (assuming sorted by normalizeSchemaDefinition)
	if !compareStringSlicesEquivalent(desired.PrimaryKey, current.PrimaryKey) { // Order should not matter for PK columns
		return &SchemaConflictError{
			ElementName:   desired.Name,
			ConflictType:  "PrimaryKeyMismatch",
			PropertyName:  "Table PrimaryKey Columns",
			ExpectedValue: fmt.Sprintf("%v", desired.PrimaryKey),
			ActualValue:   fmt.Sprintf("%v", current.PrimaryKey),
		}
	}

	// Compare table-level UniqueConstraints
	if len(desired.UniqueConstraints) != len(current.UniqueConstraints) {
		return &SchemaConflictError{ElementName: desired.Name, ConflictType: "UniqueConstraintCountMismatch", PropertyName: "Table UniqueConstraints Count", ExpectedValue: fmt.Sprintf("%d", len(desired.UniqueConstraints)), ActualValue: fmt.Sprintf("%d", len(current.UniqueConstraints))}
	}
	// This comparison assumes constraint names are stable and comparable.
	// And that columns within each constraint are sorted by normalizeSchemaDefinition.
	for name, dConstraintCols := range desired.UniqueConstraints {
		cConstraintCols, exists := current.UniqueConstraints[name]
		if !exists {
			return &SchemaConflictError{ElementName: desired.Name, ConflictType: "MissingUniqueConstraint", PropertyName: fmt.Sprintf("UniqueConstraint '%s'", name), ExpectedValue: "Exists", ActualValue: "Not Found"}
		}
		if !compareStringSlicesEquivalent(dConstraintCols, cConstraintCols) { // Order should not matter
			return &SchemaConflictError{ElementName: desired.Name, ConflictType: "UniqueConstraintMismatch", PropertyName: fmt.Sprintf("UniqueConstraint '%s' Columns", name), ExpectedValue: fmt.Sprint(dConstraintCols), ActualValue: fmt.Sprint(cConstraintCols)}
		}
	}
	for name := range current.UniqueConstraints {
		if _, exists := desired.UniqueConstraints[name]; !exists {
			return &SchemaConflictError{ElementName: desired.Name, ConflictType: "ExtraUniqueConstraint", PropertyName: fmt.Sprintf("UniqueConstraint '%s'", name), ExpectedValue: "Not Found", ActualValue: "Exists"}
		}
	}

	// If desired.SQL and current.SQL differ, but all structural checks pass,
	// it implies cosmetic differences (comments, whitespace) or ordering differences
	// that our normalization didn't catch but are structurally equivalent.
	// In this case, for AutoMigrate, we might consider them the same.
	// The strictness of SQL string comparison vs structural comparison can be tuned.
	// For now, if all structural checks pass, return nil.
	// The check `desired.SQL == current.SQL` was removed from the top to allow this detailed structural diff first.
	// If structures are identical, slight SQL variations might be permissible.
	return nil
}

// ProcessSchemaDifferences further processes a SchemaDifferences object, which has already
// had an initial categorization of differences by IdentifySchemaDifferences.
// This function focuses on the elements marked for comparison:
//
// 1.  **Tables (`diffs.ToCompareTables`)**:
//     It calls CompareTableDefinitions for each table pair.
//     - If CompareTableDefinitions returns a *SchemaConflictError (indicating a structural
//       mismatch that AutoMigrate cannot resolve), this error is collected.
//       AutoMigrate will typically halt if any such conflicts are found.
//     - If tables are structurally equivalent (CompareTableDefinitions returns nil),
//       they require no DDL action and are effectively removed from further consideration.
//     The `diffs.ToCompareTables` map is cleared after processing, as its contents
//     are either moved to `tableConflicts` or deemed equivalent.
//
// 2.  **Indexes, Views, Triggers (`diffs.ToCompareIndexes`, etc.)**:
//     For these elements, it compares their `DesiredSQL` and `CurrentSQL` (from ElementComparison).
//     - If the SQL definitions differ, the element is considered changed. The current
//       version (from `currentSchema`) is added to the appropriate `diffs.ToDelete` map,
//       and the desired version (from `desiredSchema`) is added to `diffs.ToCreate`.
//       This effectively schedules a DROP and CREATE for the element.
//     - If SQL definitions are identical, no action is needed.
//     The respective `ToCompare...` maps are cleared after processing.
//
// Parameters:
//   - diffs: The SchemaDifferences object to process. This object is modified in place.
//   - desiredSchema: The full desired schema, used to retrieve original definitions
//     for elements being moved to `ToCreate`.
//   - currentSchema: The full current schema, used for elements being moved to `ToDelete`.
//
// Returns:
//   A slice of SchemaConflictError containing all structural conflicts found for tables.
//   If this slice is non-empty, AutoMigrate should typically halt.
func ProcessSchemaDifferences(diffs *SchemaDifferences, desiredSchema, currentSchema *SchemaDefinition) []SchemaConflictError {
	var tableConflicts []SchemaConflictError

	// Process ToCompareTables
	// Create a new map for tables that are truly identical and don't need action,
	// or keep it empty if all differing tables result in conflicts or are handled.
	processedComparableTables := make(map[string]TableComparison)

	for name, comparison := range diffs.ToCompareTables {
		// It's important that comparison.Desired and comparison.Current are normalized
		// (e.g., columns sorted) before this call, if CompareTableDefinitions expects it.
		// Let's assume normalizeSchemaDefinition was called on desiredSchema & currentSchema.
		conflict := CompareTableDefinitions(comparison.Desired, comparison.Current)
		if conflict != nil {
			tableConflicts = append(tableConflicts, *conflict)
			// Table has a conflict, it remains in ToCompareTables for reporting if needed,
			// but won't be moved to ToCreate/ToDelete by this function.
			// AutoMigrate will halt if tableConflicts is not empty.
		} else {
			// Tables are structurally equivalent. No action needed for this table.
			// It can be removed from ToCompareTables as it won't be migrated.
			// Or, if ToCompareTables is purely for conflict reporting, then it means no conflict.
		}
	}
	// If AutoMigrate halts on any conflict, ToCompareTables doesn't need further pruning here.
	// If it were to proceed selectively, we'd clear matching tables.
	// For now, let's clear ToCompareTables as its purpose in the diff processing stage
	// is to feed this conflict detection. If conflicts exist, migration stops.
	// If no conflicts, these tables require no action.
	diffs.ToCompareTables = processedComparableTables


	// Process ToCompareIndexes
	// Need original definitions from desiredSchema and currentSchema for this.
	newToCompareIndexes := make(map[string]ElementComparison)
	for name, comp := range diffs.ToCompareIndexes {
		if comp.DesiredSQL != comp.CurrentSQL {
			// If SQL differs, plan to drop current and create desired.
			if currentIdx, ok := currentSchema.Indexes[name]; ok {
				diffs.ToDelete.Indexes[name] = currentIdx
			}
			if desiredIdx, ok := desiredSchema.Indexes[name]; ok {
				diffs.ToCreate.Indexes[name] = desiredIdx
			}
		} else {
			// SQL is the same, no action needed. Keep in (new) ToCompareIndexes if needed for other logic,
			// or simply don't add to ToCreate/ToDelete.
		}
	}
	diffs.ToCompareIndexes = newToCompareIndexes // Clear processed items

	// Process ToCompareViews
	newToCompareViews := make(map[string]ElementComparison)
	for name, comp := range diffs.ToCompareViews {
		if comp.DesiredSQL != comp.CurrentSQL {
			if currentView, ok := currentSchema.Views[name]; ok {
				diffs.ToDelete.Views[name] = currentView
			}
			if desiredView, ok := desiredSchema.Views[name]; ok {
				diffs.ToCreate.Views[name] = desiredView
			}
		}
	}
	diffs.ToCompareViews = newToCompareViews

	// Process ToCompareTriggers
	newToCompareTriggers := make(map[string]ElementComparison)
	for name, comp := range diffs.ToCompareTriggers {
		if comp.DesiredSQL != comp.CurrentSQL {
			if currentTrigger, ok := currentSchema.Triggers[name]; ok {
				diffs.ToDelete.Triggers[name] = currentTrigger
			}
			if desiredTrigger, ok := desiredSchema.Triggers[name]; ok {
				diffs.ToCreate.Triggers[name] = desiredTrigger
			}
		}
	}
	diffs.ToCompareTriggers = newToCompareTriggers

	return tableConflicts
}

// compareStringSlicesEquivalent checks if two string slices are equivalent (same elements, order doesn't matter).
func compareStringSlicesEquivalent(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 { // Both are empty
		return true
	}
	// Create copies, sort them, then compare
	// This is important for things like PK columns or unique constraint columns
	// where the order in SQL definition might not match the order in our parsed struct,
	// but semantically they are the same set.
	// Assumes normalizeSchemaDefinition has already sorted them where appropriate.
	// If not, this function should sort copies of a and b.
	// For now, let's assume they are pre-sorted if order doesn't matter,
	// or that their order *is* significant if they are not pre-sorted.
	// The current TableDefinition fields like PrimaryKey are []string, implying order.
	// If order is significant:
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
	// If order is not significant (and slices are not pre-sorted):
	// countA := make(map[string]int)
	// for _, s := range a {
	// 	countA[s]++
	// }
	// for _, s := range b {
	// 	if countA[s] == 0 {
	// 		return false
	// 	}
	// 	countA[s]--
	// }
	// return true
}


// Helper function to compare string slices (used for PKs, Unique Constraint columns)
// Not currently used as direct iteration and comparison is done, but could be useful.
func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Helper function to compare map[string][]string (used for UniqueConstraints)
// Not currently used, direct iteration is done.
func compareUniqueConstraintsMaps(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, valA := range a {
		valB, ok := b[key]
		if !ok || !compareStringSlices(valA, valB) {
			return false
		}
	}
	return true
}
