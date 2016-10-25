package dbutils

import (
	"errors"
	"bytes"
	"strings"
	"fmt"
	"github.com/argpass/dbutils/Q"
)

////////////////////// matrix /////////////////////

type matrix struct {
	Rows [][]interface{}
	NumRows int
	NumCols int
}

// buildMatrix builds a matrix
func buildMatrix(rows ...[]interface{}) (*matrix, error) {
	p := &matrix{Rows:rows}
	p.NumRows = len(rows)
	p.NumCols = 0
	if p.NumRows == 0 {
		return p, nil
	}
	p.NumCols = len(rows[0])
	for _, row := range rows {
		if len(row) != p.NumCols {
			return nil, errors.New("bad matrix:length of rows are not same")
		}
	}
	return p, nil
}

// AddRow to the matrix
func (m *matrix) AddRow(row []interface{}) (error) {
	if m.NumRows > 0 && len(row) != m.NumCols {
		return errors.New("length not match")
	}
	m.Rows = append(m.Rows, row)
	m.NumRows++
	m.NumCols = len(row)
	return nil
}

// Transpose to a new matrix
func (m *matrix) Transpose() (*matrix, error)  {
	var rows [][]interface{}
	for i:=0; i<m.NumCols; i++ {
		var row []interface{}
		for j:= 0; j<m.NumRows; j++ {
			row = append(row, m.Rows[j][i])
		}
		rows = append(rows, row)
	}
	built, err := buildMatrix(rows...)
	if err != nil {
		return nil, err
	}
	return built, nil
}

func (m *matrix) NumElem() int {
	return m.NumRows * m.NumCols
}


//////////////////////////// SQL utils /////////////////////////

type WhereMap map[string] Q.Caller

// Merge others
func (where WhereMap) Merge(others... WhereMap) {
	for _, other := range others {
		for k, v := range other {
			where[k] = v
		}
	}
}

// FieldMap is defined to manage fields map easily
// key is field name in db
type FieldMap map[string] interface{}

// Merge others
func (fm FieldMap) Merge(others... FieldMap) {
	for _, other := range others {
		for k, v := range other {
			fm[k] = v
		}
	}
}

// FieldValuesMap
//
// Sample:
// map[string][]interface{} {
//    "field_a": []interface{}{"v1","v2","v3"},
//    "field_b": []interface{}{"v1","v2","v3"},
// }
// SQL:
// INSERT INTO TABLE ("field_a", "field_b") VALUES
//      ("v1","v1"), ("v2","v2"),("v3","v3");
type FieldValuesMap map[string][]interface{}

func (w WhereMap) BuildWhereBlock(argsReceiver []interface{}) (block string, args []interface{}, ok bool) {
	args = argsReceiver
	// build sql string
	if len(w) == 0 {
		return block, args, ok
	}
	// build where block
	var whereSlice []string
	for name, caller := range w {
		block, args = caller.Call(name, args)
		whereSlice = append(whereSlice, block)
	}
	block = strings.Join([]string{"WHERE", strings.Join(whereSlice, "AND")}, " ")
	ok = true
	return block, args, ok
}

// BuildUpdateSQL builds SQL for updating rows
func BuildUpdateSQL(table string, fieldsMap FieldMap,
		whereMap WhereMap) (query string, args []interface{}, err error)  {
	if len(fieldsMap) == 0 {
		err = NO_UPDATE_FIELDS
		return "", nil, err
	}
	// build set block
	var setSlice []string
	for name, value := range fieldsMap {
		setSlice = append(setSlice, strings.Join([]string{name,"?"}, "="))
		args = append(args, value)
	}
	setBlock :=strings.Join(setSlice, ",")

	// build sql string
	var whereBlock string
	var ok bool
	if whereBlock, args, ok = whereMap.BuildWhereBlock(args); ok {
		query = strings.Join([]string{"UPDATE", table, "SET", setBlock, whereBlock}, " ")
	}else {
		query = strings.Join([]string{"UPDATE", table, "SET", setBlock}, " ")
	}
	return query, args, nil
}

// BuildDeleteSQL builds SQL for deleting rows
func BuildDeleteSQL(table string, where WhereMap)(query string, args []interface{}) {
	var whereBlock string
	var ok bool
	if whereBlock, args, ok = where.BuildWhereBlock(args); ok {
		query = strings.Join([]string{"DELETE", "FROM", table, whereBlock}, " ")
	}else {
		query = strings.Join([]string{"DELETE", "FROM", table}, " ")
	}
	return query, args
}

// BuildInsertSQL builds SQL for inserting the fieldsMap
func BuildInsertSQL(table string, fieldsMap FieldMap) (query string, args []interface{}, err error) {
	var fieldsSlice []string
	var valuesSlice []string
	for field, value := range fieldsMap {
		fieldsSlice = append(fieldsSlice, field)
		valuesSlice = append(valuesSlice, "?")
		args = append(args, value)
	}
	if len(fieldsSlice) == 0 {
		err = NO_INSERT_FIELDS
		return query, args, err
	}
	fieldsBlock := strings.Join(fieldsSlice, ",")
	valuesBlock := strings.Join(valuesSlice, ",")
	query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, fieldsBlock, valuesBlock)
	return query, args, nil
}

// BuildInsertManySQL
// Input Sample:
// map[string][]interface{} {
//    "field_a": []interface{}{"v1","v2","v3"},
//    "field_b": []interface{}{"v1","v2","v3"},
// }
// SQL:
// INSERT INTO TABLE ("field_a", "field_b") VALUES
//      ("v1","v1"), ("v2","v2"),("v3","v3");
func BuildInsertManySQL(table string, fieldValues FieldValuesMap) (query string, args[]interface{}, err error) {
	var fieldNames []string
	m, _ := buildMatrix()
	for name, values := range fieldValues {
		m.AddRow(values)
		fieldNames = append(fieldNames, name)
	}
	if len(fieldNames) == 0 {
		err = NO_INSERT_FIELDS
		return query, args, err
	}

	m, _ = m.Transpose()

	// make []byte `(?,?,?,...),`
	var aValueBlock []byte
	s := bytes.Repeat([]byte{'?',','}, len(fieldNames))
	// make last char ')'
	s[len(fieldNames) * 2 - 1] = ')'
	// make space to hold `(s,` => `(?,?,?,...),`
	aValueBlock = make([]byte, len(s) + 2)
	copy(aValueBlock[1:], s)
	aValueBlock[0] = '('
	aValueBlock[len(aValueBlock) - 1] = ','

	// make []byte `(?,?,?,...),(?,?,?,...),...)`
	valuesBlock := bytes.Repeat(aValueBlock, m.NumRows)
	// replace last byte ',' to space char ' '
	valuesBlock[len(valuesBlock) - 1] = ' '

	args = make([]interface{}, m.NumElem())
	copied := 0
	for _, row := range m.Rows {
		copied += copy(args[copied:], row)
	}
	fieldsBlock := strings.Join(fieldNames, ",")
	query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, fieldsBlock, string(valuesBlock))
	return query, args, nil
}

// BuildQuerySQL builds sql for querying rows match `where`
// todo: support order by, asc/desc, name as,... options
func BuildQuerySQL(table string, where WhereMap,
		fieldNames []string, limit Q.Limit) (query string, args []interface{})  {
	if len(fieldNames) == 0 {
		fieldNames = append(fieldNames, "*")
	}
	fields := strings.Join(fieldNames, ",")
	blocks := []string{fmt.Sprintf("SELECT %s FROM %s", fields, table)}

	var whereBlock string
	var ok bool
	if whereBlock, args, ok = where.BuildWhereBlock(args); ok {
		blocks = append(blocks, whereBlock)
	}

	if ! limit.IsEmpty() {
		limitBlock := fmt.Sprintf(" LIMIT %d, %d", limit.Begin(), limit.MaxNum())
		blocks = append(blocks, limitBlock)
	}

	query = strings.Join(blocks, " ")
	return query, args
}

