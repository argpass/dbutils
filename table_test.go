package dbutils

import (
	"testing"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"os"
	"fmt"
	"github.com/argpass/dbutils/Q"
	"github.com/argpass/dbutils/evt"
)

func init()  {
	// todo: remove this constant env key here
	os.Setenv("DBUTILS_MYSQL_DNS", "root:akun@123@(vagrant:3306)/test?charset=utf8")

	evt.Subscribe((*SQLEvent)(nil),
		evt.EventHandler(func(e evt.Event)(result interface{}){
			if ev, ok := e.(*SQLEvent); ok{
				fmt.Println("[SQL]:",ev.Query, ",args:",ev.Args)
			}
			return nil
	        }))

	ConnectAll()
}

// clean testing tx
func clean (tx *sqlx.Tx){
	re := recover()
	var e error
	var ok bool
	if e, ok = re.(error); ok {
		fmt.Println("panic:", e)
		tx.Rollback()
	}else{
		tx.Commit()
	}
	if e != nil {
		panic(e)
	}
}

var TestMysql = true
var mysqlDB *sqlx.DB

type Scheme struct {
	create string
	drop string
}

func (p Scheme) Mysql() (string, string) {
	return p.create, p.drop
}

var t_book = "dbutils_t_book"

var test_scheme = Scheme{
	create:
	`CREATE TABLE dbutils_t_book(id INTEGER AUTO_INCREMENT PRIMARY KEY,name VARCHAR(100) NOT NULL,
	tag TINYINT NULL, deleted BOOLEAN DEFAULT FALSE)`,
	drop:"drop table dbutils_t_book",
}

type testFunc func(db *sqlx.DB, t *testing.T)

func RunWithScheme(scheme Scheme, t *testing.T, testFn testFunc)  {
	runner := func(db *sqlx.DB, create string, drop string){
		// drop tables
		defer func(){
			db.MustExec(drop)
			db.Close()
		}()
		// prepare environment
		db.MustExec(create)
		// run test function
		testFn(mysqlDB, t)
	}

	if TestMysql {
		create, drop := scheme.Mysql()
		runner(mysqlDB, create, drop)
	}
}

func ConnectAll()  {
	mysqlDNS := os.Getenv("DBUTILS_MYSQL_DNS")

	if TestMysql {
		db, err := sqlx.Open("mysql", mysqlDNS)
		if err != nil {
			fmt.Printf("\nfail to connect mysql, err:%v\n", err)
			TestMysql = false
		}
		mysqlDB = db
	}
}

func TestSimpleTable_Insert(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		_, err := table.Insert(FieldMap{"name": "Python"})
		if err != nil {
			t.Fatalf("\n[insert] err:%v\n", err)
		}
		tx.Commit()
	})
}

func TestSimpleTable_Get(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python", "tag": 1, "deleted": true})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		row, err := table.Get(nil, WhereMap{"name": Q.EQ("Python")})

		var r Result
		r, err = row.GetResult()
		name, err := r.GetString("name")
		if name != "Python" {
			t.Fatalf("\n expect name Python got %s\n", name)
		}

		if err != nil {
			t.Fatalf("\n[get] err:%v\n", err)
		}
		var rs = map[string] interface{}{}
		row, err = table.Get([]string{"name"}, WhereMap{"name": Q.EQ("No")})
		if err != nil {
			t.Fatalf("\n[get] err:%v\n", err)
		}
		rs = map[string] interface{}{}
		row.MapScan(rs)
		if len(rs) != 0 {
			t.Fatalf("\n[get] expect 0 row got %d\n", len(rs))
		}
		tx.Commit()
	})
}

func TestResult_GetXXX(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python", "tag": 1, "deleted": true})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		row, err := table.Get(nil, WhereMap{"name": Q.EQ("Python")})

		var r Result
		r, err = row.GetResult()
		name, err := r.GetString("name")
		if name != "Python" {
			t.Fatalf("\n expect name Python got %s\n", name)
		}
		var id int64
		var deleted bool
		var tag int
		id, err = r.GetInt64("id")
		if err != nil {
			t.Fatalf("\n fail to get id, err:%v\n", err)
		}
		if id != 1 {
			t.Fatalf("\n invalid id value %d\n", id)
		}
		deleted, err = r.GetBool("deleted")
		if err != nil {
			t.Fatalf("\n fail to get deleted field, err:%v\n", err)
		}
		if deleted != true {
			t.Fatalf("\n invalid deleted field value %v\n", deleted)
		}
		tag, err = r.GetInt("tag")
		if err != nil {
			t.Fatalf("\n fail to get tag field, err:%v\n", err)
		}
		if tag != 1 {
			t.Fatalf("\n invalid tag value :%d\n", tag)
		}

		tx.Commit()
	})
}

func TestSimpleTable_Query_IN_NI(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		rows, err := table.Query([]string{"name"}, WhereMap{"name": Q.IN([]interface{}{"Python", "Golang"})})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		var rs []interface{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		fmt.Println(rs)
		if len(rs) != 2 {
			t.Fatalf("\n[query in] expect 2 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"name": Q.NI([]interface{}{"Python", "Golang"})})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		fmt.Println(rs)
		if len(rs) != 1 {
			t.Fatalf("\n[query in] expect 2 got %d\n", len(rs))
		}
		tx.Commit()
	})
}

func TestSimpleTable_Query_Null_Not_Null(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		rows, err := table.Query([]string{"name"}, WhereMap{"name": Q.NotNull()})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		var rs []interface{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 3 {
			t.Fatalf("\n[query in] expect 3 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"name": Q.IsNull()})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 0 {
			t.Fatalf("\n[query in] expect 0 got %d\n", len(rs))
		}
		tx.Commit()
	})
}

func TestSimpleTable_Query_Like(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		rows, err := table.Query([]string{"name"}, WhereMap{"name": Q.Like("Py")})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		var rs []interface{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 1 {
			t.Fatalf("\n[query in] expect 1 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"name": Q.NotLike("Py")})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 2 {
			t.Fatalf("\n[query in] expect 2 got %d\n", len(rs))
		}
		tx.Commit()
	})
}

func TestSimpleTable_Query_EQ_NE_GT_LT(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})
		table.Insert(FieldMap{"name": "C++"})
		table.Insert(FieldMap{"name": "Java"})

		rows, err := table.Query([]string{"name"}, WhereMap{"id": Q.EQ(1)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		var rs []interface{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 1 {
			t.Fatalf("\n[query] expect 1 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"id": Q.NE(1)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 4 {
			t.Fatalf("\n[query] expect 4 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"id": Q.GT(4)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 1 {
			t.Fatalf("\n[query] expect 1 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"id": Q.GTE(4)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 2 {
			t.Fatalf("\n[query] expect 2 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"id": Q.LT(4)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 3 {
			t.Fatalf("\n[query] expect 3 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"id": Q.LTE(4)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 4 {
			t.Fatalf("\n[query] expect 4 got %d\n", len(rs))
		}
		tx.Commit()
	})
}

func TestSimpleTable_Query_Between(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		rows, err := table.Query([]string{"name"}, WhereMap{"id": Q.Between(1,2)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		var rs []interface{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 2 {
			t.Fatalf("\n[query in] expect 2 got %d\n", len(rs))
		}

		rows, err = table.Query([]string{"name"}, WhereMap{"id": Q.NotBetween(1,2)})
		if err != nil {
			t.Fatalf("\n[query] err:%v\n", err)
		}
		rs = []interface{}{}
		for rows.Next() {
			result := map[string]interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		rows.Close()
		if len(rs) != 1 {
			t.Fatalf("\n[query in] expect 1 got %d\n", len(rs))
		}
		tx.Commit()
	})
}


func TestSimpleTable_Update(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		var newName = "NewBook"
		cnt, err := table.Update(FieldMap{"name": newName},
			WhereMap{"name": Q.EQ("Python")})
		if err != nil {
			t.Fatalf("\n[update] err:%v\n", err)
		}
		if cnt != 1 {
			t.Fatalf("\n expect cnt 2 got %d\n", cnt)
		}
		rows, _ := table.Query(nil, WhereMap{"name": Q.EQ(newName)})
		var rs []interface{}
		for rows.Next() {
			var result = map[string] interface{}{}
			rows.MapScan(result)
			rs = append(rs, result)
		}
		fmt.Println(rs)
		if len(rs) != int(cnt) {
			t.Fatalf("\n expect len %d got %d\n", cnt, len(rs))
		}
	})
}

func TestSimpleTable_Delete(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		// prepare 3 rows
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		cnt, err := table.Delete(WhereMap{"name": Q.EQ("Python")})
		if err != nil {
			t.Fatalf("\n[insert] err:%v\n", err)
		}
		if cnt != 1 {
			t.Fatalf("\n[delete] expect to delete cnt 1 got %d\n", cnt)
		}

		cnt, _ = table.Delete()
		if cnt != 2 {
			t.Fatalf("\n[delete] expect to delete cnt 2 got %d\n", cnt)
		}

		tx.Commit()
	})
}

func TestQ(t *testing.T) {
	RunWithScheme(test_scheme, t, func(db *sqlx.DB, t *testing.T){
		tx, _ := db.Beginx()
		defer clean(tx)
		table := NewSimpleTable(tx, t_book)
		// prepare 3 rows
		table.Insert(FieldMap{"name": "Python"})
		table.Insert(FieldMap{"name": "Golang"})
		table.Insert(FieldMap{"name": "Ruby"})

		table.Query([]string{"name"}, WhereMap{"id": Q.EQ(2)})
	})
}

