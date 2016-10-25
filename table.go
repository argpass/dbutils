package dbutils

import (
	"github.com/jmoiron/sqlx"
	"errors"
	"database/sql"
	"github.com/argpass/dbutils/Q"
	"github.com/argpass/dbutils/evt"
	"fmt"
	"golang.org/x/tools/container/intsets"
)

// NO_INSERT_FIELDS is exception when trying to build insert sql with no insert fields
var NO_INSERT_FIELDS = errors.New("no insert fields")

// NO_UPDATE_FIELDS is exception when trying to build update sql with no insert fields
var NO_UPDATE_FIELDS = errors.New("no update fields")

// Result is a map type holding data of a row
// I will serve some methods to get data easily
// all integer in db will be returned as int64
// todo: the converting rule of other db(psql, sqlite) ?
type Result map[string] interface{}

func (p Result) get(name string) (value interface{}, err error)  {
	v, ok := p[name]
	if !ok {
		err = fmt.Errorf("no field %s", name)
		return value, err
	}
	value = v
	return value, err
}

// GetBool picks bool value of `name`
// it converts int64, int value to bool (if value is 0 or 1)
func (p Result) GetBool(name string) (value bool, err error)  {
	var v interface{}
	v, err = p.get(name)
	if err != nil {
		return value, err
	}
	switch tp:=v.(type) {
	case bool:
		value = tp
	case int64, int:
		var i int
		if j64, ok := v.(int64); ok {
			i = int(j64)
		}else {
			i = v.(int)
		}
		if i != 0 && i != 1 {
			err = fmt.Errorf("no bool `%s`", name)
		}else{
			if i == 0 {
				value = false
			}
			value = true
		}
	default:
		err = fmt.Errorf("no bool `%s`", name)
	}
	return value, err
}

func (p Result) GetInt64(name string) (value int64, err error)  {
	v, ok := p[name]
	if !ok {
		err = fmt.Errorf("no field %s", name)
		return value, err
	}
	switch tp:=v.(type) {
	case int64:
		value = tp
	default:
		err = fmt.Errorf("no int64 %s", name)
	}
	return value, err
}

// GetInt picks Int value of `name`
// it just casts int64 value to int
func (p Result) GetInt(name string) (value int, err error)  {
	var v int64
	v, err = p.GetInt64(name)
	if v >= int64(intsets.MaxInt) {
		err = errors.New("int overflow")
		return value, err
	}
	if err != nil {
		return value, err
	}
	value = int(v)
	return value, err
}

// GetString picks string value of `name`
// it just converts []byte value to string
func (p Result) GetString(name string) (value string, err error)  {
	var v []byte
	v, err = p.GetBytes(name)
	if err != nil {
		return value, err
	}
	value = string(v)
	return value, err
}

// GetBytes picks []bytes value of `name`
func (p Result) GetBytes(name string) (value []byte, err error)  {
	v, ok := p[name]
	if !ok {
		err = fmt.Errorf("no field %s", name)
		return value, err
	}
	switch tp:=v.(type) {
	case []byte:
		value = tp
	default:
		err = fmt.Errorf("no []byte %s", name)
	}
	return value, err
}

// Row is wrapper of `sqlx.Row`
type Row struct {
	*sqlx.Row
}

// GetResult scans current row as `Result`
func (p *Row) GetResult() (result Result, err error){
	var d = map[string] interface{}{}
	err = p.MapScan(d)
	result = Result(d)
	return result, err
}

// Rows is wrapper of `sqlx.Rows`
type Rows struct {
	*sqlx.Rows
}

// GetResult scans current row as `Result`
func (p *Rows) GetResult() (result Result, err error){
	var d = map[string] interface{}{}
	err = p.MapScan(d)
	result = Result(d)
	return result, err
}

// SQLEvent ought to be triggered every sql executed
type SQLEvent struct {
	Query string
	Args []interface{}
	Result sql.Result
	Error error
}

// SimpleTable is a tool to operate db table easily
type SimpleTable struct {
	tx       *sqlx.Tx
	table    string
}

// NewSimpleTable create new instance of `SimpleTable`
func NewSimpleTable(tx *sqlx.Tx, tableName string) (*SimpleTable) {
	p := &SimpleTable{tx:tx, table:tableName}
	return p
}

// Exec wraps `p.tx.Exec` to handle callback func
// I can print logs or do something else with callback func
func (p *SimpleTable) Exec(query string, args...interface{}) (result sql.Result, err error) {
	result, err = p.tx.Exec(query, args...)
	// build sql event and send to subscribers
	event := &SQLEvent{Query:query, Args:args, Result:result, Error:err}
	evt.SynSend(event)
	return result, err
}

// Insert method inserts a row in db and return id
func (p *SimpleTable) Insert(fieldsMap FieldMap) (id int64, err error) {
	var query string
	var args []interface{}
	var result sql.Result

	query, args, err = BuildInsertSQL(p.table, fieldsMap)
	if err != nil {
		return id, err
	}
	result, err = p.Exec(query, args...)
	if err != nil {
		return id, err
	}
	return result.LastInsertId()
}

// InsertMany method inserts more than one rows in db and return the last id
func (p *SimpleTable) InsertMany(valuesMap FieldValuesMap) (lastID int64, err error)  {
	var query string
	var args []interface{}
	var result sql.Result

	query, args, err = BuildInsertManySQL(p.table, valuesMap)
	if err != nil {
		return lastID, err
	}
	result, err = p.Exec(query, args...)
	if err != nil {
		return lastID, err
	}
	return result.LastInsertId()
}

// Update rows match `where`
func (p *SimpleTable) Update(fieldsMap FieldMap, where...WhereMap) (affected int64, err error) {
	var query string
	var result sql.Result
	var args []interface{}
	var whereMap = WhereMap{}
	whereMap.Merge(where...)

	query, args, err = BuildUpdateSQL(p.table, fieldsMap, whereMap)
	result, err = p.Exec(query, args...)
	if err != nil {
		return affected, err
	}
	return result.RowsAffected()
}

// Delete rows match `where`
func (p *SimpleTable) Delete(where...WhereMap) (affected int64, err error)  {
	var query string
	var result sql.Result
	var args []interface{}
	var whereMap = WhereMap{}
	whereMap.Merge(where...)

	query, args = BuildDeleteSQL(p.table, whereMap)
	result, err = p.Exec(query, args...)
	if err != nil {
		return affected, err
	}
	return result.RowsAffected()
}

// Get one matches `where`
// return nil if no one matches
//
// Example:
//   var row Row
//   var fields = []string{"name", "age"}
//
//   // select name, age from t_table where age > 10 limit 1
//   row, err = Query(fields, WhereMap{"age": Q.GTE(10)})
//
//   // query all fields: select * from t_table where age > 10 limit 1
//   row, err = Query(nil, WhereMap{"age": Q.GTE(10)})
//
func (p *SimpleTable) Get(fieldNames []string, where ...WhereMap) (row *Row, err error) {
	var whereMap = WhereMap{}
	whereMap.Merge(where...)
	query, args := BuildQuerySQL(p.table, whereMap, fieldNames, Q.Limit{0, 1})
	row = &Row{(p.tx.QueryRowx(query, args...))}
	// send sql event
	event := &SQLEvent{Query:query, Args:args, Result:nil, Error:err}
	evt.SynSend(event)
	return row, err
}

// Query rows match `where`
//
// Example:
//
//   var rows Rows
//
//   // select name, age from t_table where age > 10
//   var fields = []string{"name", "age"}
//   rows, err = Query(fields, WhereMap{"age": Q.GTE(10)})
//
//   // query all fields: select * from t_table where age > 10
//   rows, err = Query(nil, WhereMap{"age": Q.GTE(10)})
//
func (p *SimpleTable) Query(fieldNames []string, where ...WhereMap) (rows *Rows, err error)  {
	var whereMap = WhereMap{}
	whereMap.Merge(where...)
	query, args := BuildQuerySQL(p.table, whereMap, fieldNames, Q.Limit{})
	var rs *sqlx.Rows
	rs, err = p.tx.Queryx(query, args...)
	rows = &Rows{rs}
	// send sql event
	event := &SQLEvent{Query:query, Args:args, Result:nil, Error:err}
	evt.SynSend(event)
	return rows, err
}

// Use is the method to get an instance of `SimpleTable`
// it just calls `NewSimpleTable` method to build an new instance
// I will make `SimpleTable` objects pooled in future (maybe ^_^)
func Use(tx *sqlx.Tx, tableName string) (*SimpleTable){
	return NewSimpleTable(tx, tableName)
}

