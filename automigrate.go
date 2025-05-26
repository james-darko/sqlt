package sqlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	rsql "github.com/rqlite/sql"
)

// getTableNameForDependent returns the table name an object depends on.
func getTableNameForDependent(stmt rsql.Statement) string {
	switch s := stmt.(type) {
	case *rsql.CreateIndexStatement:
		if s.Table != nil {
			return s.Table.Name
		}
	case *rsql.CreateTriggerStatement:
		if s.Table != nil {
			return s.Table.Name
		}
	case *rsql.CreateViewStatement:
		return ""
	}
	return ""
}

func AutoMigrateFromEnv(ctx context.Context, db DB, allowTableDeletes bool) error {
	schemaFile, ok := os.LookupEnv("DATABASE_SCHEMA")
	if !ok {
		return fmt.Errorf("DATABASE_SCHEMA env var not found")
	}
	schema, err := os.Open(schemaFile)
	if err != nil {
		return fmt.Errorf("could not open schema file %s: %w", schemaFile, err)
	}
	defer schema.Close()
	return AutoMigrate(ctx, db, schema, allowTableDeletes)
}

// AutoMigrate automatically adjusts the database schema to match the provided schema.
func AutoMigrate(ctx context.Context, db DB, schema io.Reader, allowTableDeletes bool) error {
	return db.Txc(ctx, func(tx Tx) error {
		dbObjects := make(map[string]rsql.Statement)
		schemaObjectsMap := make(map[string]rsql.Statement)
		processedSchemaObjects := make(map[string]bool)
		rebuiltTables := make(map[string]bool)
		tablesToDropIfDisallowed := []string{} // Moved to top to collect all table drop violations

		dbMasterRows, err := masterRows(ctx, tx)
		if err != nil {
			return fmt.Errorf("AutoMigrate: could not get master rows from DB: %w", err)
		}
		for _, row := range dbMasterRows {
			if strings.HasPrefix(row.Name, "sqlite_") {
				continue
			}
			parser := rsql.NewParser(strings.NewReader(row.Sql))
			stmt, parseErr := parser.ParseStatement()
			if parseErr != nil {
				if strings.Contains(row.Name, "sqlite_sequence") {
					continue
				}
				return fmt.Errorf("AutoMigrate: could not parse SQL for DB object %s (SQL: %s): %w", row.Name, row.Sql, parseErr)
			}
			dbObjects[strings.ToLower(row.Name)] = stmt
		}

		var schemaStmtsInOrder []rsql.Statement
		schemaParser := rsql.NewParser(schema)
		for {
			sStmt, parseErr := schemaParser.ParseStatement()
			if errors.Is(parseErr, io.EOF) {
				break
			}
			if parseErr != nil {
				return fmt.Errorf("AutoMigrate: could not parse statement from input schema: %w", parseErr)
			}

			switch sStmt.(type) {
			case *rsql.SelectStatement, *rsql.InsertStatement, *rsql.UpdateStatement, *rsql.DeleteStatement:
				continue
			default:
				schemaStmtsInOrder = append(schemaStmtsInOrder, sStmt)
			}
		}

		for _, sStmt := range schemaStmtsInOrder {
			sNameOriginal, nameErr := getStatementName(sStmt)
			if nameErr != nil {
				return fmt.Errorf("AutoMigrate: could not extract name from schema statement %s: %w", sStmt.String(), nameErr)
			}
			sNameLower := strings.ToLower(sNameOriginal)
			if _, exists := schemaObjectsMap[sNameLower]; exists {
				return fmt.Errorf("AutoMigrate: duplicate object name '%s' found in schema definition", sNameOriginal)
			}
			schemaObjectsMap[sNameLower] = sStmt
		}

		for _, sStmt := range schemaStmtsInOrder {
			sNameOriginal, _ := getStatementName(sStmt)
			sNameLower := strings.ToLower(sNameOriginal)
			processedSchemaObjects[sNameLower] = true

			dStmt, dExistsInDbInitially := dbObjects[sNameLower]

			_, sIsTable := sStmt.(*rsql.CreateTableStatement)

			forceRecreateDueToDependency := false
			if !sIsTable {
				dependentTableName := getTableNameForDependent(sStmt)
				if dependentTableName != "" && rebuiltTables[strings.ToLower(dependentTableName)] {
					forceRecreateDueToDependency = true
				}
			}

			if !dExistsInDbInitially {
				if _, execErr := tx.Exec(sStmt.String()); execErr != nil {
					return fmt.Errorf("AutoMigrate: error creating new object %s: %w. SQL: %s", sNameOriginal, execErr, sStmt.String())
				}
				if sIsTable {
					rebuiltTables[sNameLower] = true
				}
			} else {
				matchType, diffDescription, cmpErr := compareStatements(dStmt, sStmt)
				if cmpErr != nil {
					return fmt.Errorf("AutoMigrate: error comparing object '%s': %w", sNameOriginal, cmpErr)
				}

				if forceRecreateDueToDependency {
					var dropSQLForRecreate string
					originalDNameForDrop, _ := getStatementName(dStmt)
					qOriginalDNameForDrop := quoteIdent(originalDNameForDrop)
					dbObjTypeForRecreate := getObjectType(dStmt)

					// Check if this forced recreate involves dropping a table when not allowed
					if dbObjTypeForRecreate == "TABLE" && !allowTableDeletes {
						tablesToDropIfDisallowed = append(tablesToDropIfDisallowed, originalDNameForDrop)
						// Do not proceed with drop or recreate for this object
						// Mark that this schema object was not successfully reconciled
						processedSchemaObjects[sNameLower] = false // Revert processing status
						continue
					}

					switch dStmt.(type) {
					case *rsql.CreateTableStatement:
						dropSQLForRecreate = fmt.Sprintf("DROP TABLE %s", qOriginalDNameForDrop)
					case *rsql.CreateIndexStatement:
						dropSQLForRecreate = fmt.Sprintf("DROP INDEX %s", qOriginalDNameForDrop)
					case *rsql.CreateViewStatement:
						dropSQLForRecreate = fmt.Sprintf("DROP VIEW %s", qOriginalDNameForDrop)
					case *rsql.CreateTriggerStatement:
						dropSQLForRecreate = fmt.Sprintf("DROP TRIGGER %s", qOriginalDNameForDrop)
					default:
						return fmt.Errorf("AutoMigrate: unknown type for DB object %s to drop for forced recreate", originalDNameForDrop)
					}

					if _, err := tx.Exec(dropSQLForRecreate); err != nil {
						errStr := strings.ToLower(err.Error())
						dbObjTypeStrLower := strings.ToLower(dbObjTypeForRecreate)
						if !(strings.HasPrefix(errStr, "no such "+dbObjTypeStrLower) ||
							(dbObjTypeForRecreate == "TABLE" && strings.Contains(errStr, "no such table")) ||
							(dbObjTypeForRecreate == "INDEX" && strings.Contains(errStr, "no such index")) ||
							(dbObjTypeForRecreate == "VIEW" && strings.Contains(errStr, "no such view")) ||
							(dbObjTypeForRecreate == "TRIGGER" && strings.Contains(errStr, "no such trigger"))) {
							return fmt.Errorf("AutoMigrate: error dropping DB object %s %s for forced recreate: %w", dbObjTypeForRecreate, originalDNameForDrop, err)
						}
					}
					if _, execErr := tx.Exec(sStmt.String()); execErr != nil {
						return fmt.Errorf("AutoMigrate: error recreating object %s after forced drop: %w. SQL: %s", sNameOriginal, execErr, sStmt.String())
					}
				} else {
					switch matchType {
					case statementMatchExact:
					case statementMatchReorderNeeded:
						if schemaTableStmt, ok := sStmt.(*rsql.CreateTableStatement); ok {
							tempTableNameSuffix := "_temp_reorder_sqlt"
							tempTableName := sNameOriginal + tempTableNameSuffix
							qOldTableName := quoteIdent(sNameOriginal)
							qTempTableName := quoteIdent(tempTableName)
							if _, err := tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qOldTableName, qTempTableName)); err != nil {
								return fmt.Errorf("AutoMigrate: error renaming table %s to %s for reorder: %w", sNameOriginal, tempTableName, err)
							}
							if _, err := tx.Exec(sStmt.String()); err != nil {
								tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qTempTableName, qOldTableName))
								return fmt.Errorf("AutoMigrate: error creating new table %s for reorder: %w. SQL: %s", sNameOriginal, err, sStmt.String())
							}
							colNames := make([]string, len(schemaTableStmt.Columns))
							for i, colDef := range schemaTableStmt.Columns {
								colNames[i] = quoteIdent(colDef.Name.Name)
							}
							joinedColNames := strings.Join(colNames, ", ")
							insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", qOldTableName, joinedColNames, joinedColNames, qTempTableName)
							if _, err := tx.Exec(insertSQL); err != nil {
								return fmt.Errorf("AutoMigrate: error copying data to reordered table %s: %w. SQL: %s", sNameOriginal, err, insertSQL)
							}
							if _, err := tx.Exec(fmt.Sprintf("DROP TABLE %s", qTempTableName)); err != nil {
								return fmt.Errorf("AutoMigrate: error dropping temporary table %s for reorder: %w", tempTableName, err)
							}
							rebuiltTables[sNameLower] = true
						} else {
							return fmt.Errorf("AutoMigrate: internal error - statementMatchReorderNeeded for non-table object %s", sNameOriginal)
						}
					case statementMatchNoMatch:
						var dIsTable bool
						if _, ok := dStmt.(*rsql.CreateTableStatement); ok {
							dIsTable = true
						}

						if sIsTable && dIsTable {
							return &SchemaConflictError{ObjectName: sNameOriginal, ObjectType: "TABLE", ExpectedSQL: sStmt.String(), ActualSQL: dStmt.String(), ConflictDetails: diffDescription}
						} else {
							dNameOriginalForDrop, _ := getStatementName(dStmt)
							if dIsTable && !allowTableDeletes {
								tablesToDropIfDisallowed = append(tablesToDropIfDisallowed, dNameOriginalForDrop)
								processedSchemaObjects[sNameLower] = false // This schema object cannot be reconciled
								continue                                   // Skip drop and create
							}

							var dropSQLNoMatch string
							qDNameOriginalForDrop := quoteIdent(dNameOriginalForDrop)
							dbObjTypeForDrop := getObjectType(dStmt)

							switch dStmt.(type) {
							case *rsql.CreateTableStatement:
								dropSQLNoMatch = fmt.Sprintf("DROP TABLE %s", qDNameOriginalForDrop)
							case *rsql.CreateIndexStatement:
								dropSQLNoMatch = fmt.Sprintf("DROP INDEX %s", qDNameOriginalForDrop)
							case *rsql.CreateViewStatement:
								dropSQLNoMatch = fmt.Sprintf("DROP VIEW %s", qDNameOriginalForDrop)
							case *rsql.CreateTriggerStatement:
								dropSQLNoMatch = fmt.Sprintf("DROP TRIGGER %s", qDNameOriginalForDrop)
							default:
								return fmt.Errorf("AutoMigrate: unknown type for DB object %s to drop for type/def change", dNameOriginalForDrop)
							}
							if _, err := tx.Exec(dropSQLNoMatch); err != nil {
								return fmt.Errorf("AutoMigrate: error dropping DB object %s %s for type/def change: %w", dbObjTypeForDrop, dNameOriginalForDrop, err)
							}

							if _, execErr := tx.Exec(sStmt.String()); execErr != nil {
								return fmt.Errorf("AutoMigrate: error creating schema object %s after dropping old version: %w. SQL: %s", sNameOriginal, execErr, sStmt.String())
							}
							if sIsTable {
								rebuiltTables[sNameLower] = true
							} else if dIsTable {
								rebuiltTables[sNameLower] = true
							}
						}
					}
				}
			}
		}

		for dNameLower, dStmt := range dbObjects {
			if !processedSchemaObjects[dNameLower] {
				originalDName, nameErr := getStatementName(dStmt)
				if nameErr != nil {
					return fmt.Errorf("AutoMigrate: error getting original name for DB object to drop (key: %s): %w", dNameLower, nameErr)
				}

				objTypeStr := getObjectType(dStmt)

				isTableToDrop := false
				if _, ok := dStmt.(*rsql.CreateTableStatement); ok {
					isTableToDrop = true
				}

				if isTableToDrop && !allowTableDeletes {
					tablesToDropIfDisallowed = append(tablesToDropIfDisallowed, originalDName)
					continue
				}

				var dropSQL string
				qOriginalDName := quoteIdent(originalDName)
				switch dStmt.(type) {
				case *rsql.CreateTableStatement:
					dropSQL = fmt.Sprintf("DROP TABLE %s", qOriginalDName)
				case *rsql.CreateIndexStatement:
					dropSQL = fmt.Sprintf("DROP INDEX %s", qOriginalDName)
				case *rsql.CreateViewStatement:
					dropSQL = fmt.Sprintf("DROP VIEW %s", qOriginalDName)
				case *rsql.CreateTriggerStatement:
					dropSQL = fmt.Sprintf("DROP TRIGGER %s", qOriginalDName)
				default:
					return fmt.Errorf("AutoMigrate: unknown type for DB object %s to drop from deletion loop", originalDName)
				}

				if _, err := tx.Exec(dropSQL); err != nil {
					errStr := strings.ToLower(err.Error())
					if !(strings.HasPrefix(errStr, "no such "+strings.ToLower(objTypeStr)) || (objTypeStr == "TABLE" && strings.Contains(errStr, "no such table")) || (objTypeStr == "INDEX" && strings.Contains(errStr, "no such index")) || (objTypeStr == "VIEW" && strings.Contains(errStr, "no such view")) || (objTypeStr == "TRIGGER" && strings.Contains(errStr, "no such trigger"))) {
						return fmt.Errorf("AutoMigrate: error dropping object %s %s from database: %w", objTypeStr, originalDName, err)
					}
				}
			}
		}

		if len(tablesToDropIfDisallowed) > 0 {
			return ErrTableDeletionNotAllowed{Tables: tablesToDropIfDisallowed}
		}

		return nil
	})
}
