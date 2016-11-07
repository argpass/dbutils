package dbutils

import (
	"testing"
)

func TestBuildInsertSQL(t *testing.T) {
	query, args, err := BuildInsertSQL("t_table", FieldMap{"name": "python"})
	if err != nil {
		t.Fatalf("err:%v", err)
	}
	t.Logf("query:%s", query)
	if args[0] != "python" {
		t.Fatalf("args is wrong:%v", args)
	}
}

func TestBuildInsertManySQL(t *testing.T) {
	query, args, err := BuildInsertManySQL("t_table", FieldValuesMap{
		"age": []interface{}{99, 8},
		"name": []interface{}{"python", "golang"},
	})
	if err != nil {
		t.Fatalf("err:%v", err)
	}
	t.Logf("query:%s", query)
	t.Logf("args:%v", args)
}

func TestMatrix_build(t *testing.T)  {
	var rows = [][]interface{}{
		[]interface{}{"a1", "a2"},
		[]interface{}{"b1", "b2", "b3"},
	}
	_, err := buildMatrix(rows...)
	if err == nil {
		t.Fatalf("expect err, but got nil")
	}

	// no data
	_, err = buildMatrix()
	if err != nil {
		t.Fatalf("can not build empty matrix")
	}

}

func TestMatrix_AddRow(t *testing.T) {
	m, err := buildMatrix()
	if err != nil {
		t.Fatalf("err:%v", err)
	}
	err = m.AddRow([]interface{}{"a1","a2"})
	if err != nil {
		t.Fatalf("fail to add row:%v", err)
	}
	err = m.AddRow([]interface{}{"b1","b2"})
	if err != nil {
		t.Fatalf("fail to add row2:%v", err)
	}

	t.Logf("matrix:%v", m)
	if m.Rows[1][1] != "b2" {
		t.Fatalf("add  fail")
	}
}

func TestMatrix_Transpose(t *testing.T) {
	var rows = [][]interface{}{
		[]interface{}{"a1", "a2", "a3"},
		[]interface{}{"b1", "b2", "b3"},
	}
	m, err := buildMatrix(rows...)
	if err != nil {
		t.Fatalf("err:%v", err)
	}
	t.Logf("matrix:%v", m)

	m, err = m.Transpose()
	if err != nil {
		t.Fatalf("err:%v", err)
	}
	t.Logf("matrix transposed:%v", m)
	if m.Rows[1][1] != "b2" {
		t.Fatalf("transpose fail")
	}
	if m.Rows[1][0] != "a2" {
		t.Fatalf("transpose fail")
	}
	if m.Rows[2][1] != "b3" {
		t.Fatalf("transpose fail")
	}
}
