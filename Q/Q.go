// Package Q support query expressions
// todo: 2.order by name asc/desc
package Q

import (
	"strings"
	"fmt"
	"bytes"
)

type Caller interface {
	Call(fieldName string, argsCollector []interface{}) (string, []interface{})
}

type ExprFn func() (string)

type QExpr struct {
	Args    interface{}
	Expr    interface{}
	NumArgs int
}

var _ Caller = &QExpr{}

func (p *QExpr) Call(fieldName string, argsCollector []interface{}) (string, []interface{})  {
	var block string = ""
	if fn, ok := p.Expr.(ExprFn); ok {
		block = strings.Join([]string{fieldName, fn()}, " ")
	}else {
		block = strings.Join([]string{fieldName, p.Expr.(string)}, " ")
	}
	if args, ok := p.Args.([]interface{}); ok {
		for _, arg := range args {
			argsCollector = append(argsCollector, arg)
		}
	}else {
		// nil Args will be ignored
		if p.Args != nil {
			argsCollector = append(argsCollector, p.Args)
		}
	}
	return  block, argsCollector
}

func EQ(v interface{}) Caller {
	return &QExpr{v, " = ?", 1}
}

func NE(v interface{}) Caller {
	return &QExpr{v, " != ?", 1}
}

func GT(v interface{}) Caller {
	return &QExpr{v, " > ?", 1}
}

func GTE(v interface{}) Caller {
	return &QExpr{v, " >= ?", 1}
}

func LT(v interface{}) Caller {
	return &QExpr{v, " < ?", 1}
}

func LTE(v interface{}) Caller {
	return &QExpr{v, " <= ?", 1}
}

func IN(v []interface{}) Caller {
	return &QExpr{v, ExprFn(func()(string) {
		// make string `IN (?,?,?,...)`
		bn := len(v) * 2 + 1
		b := make([]byte, bn)
		copy(b[1:], bytes.Repeat([]byte{'?',','}, len(v)))
		b[0], b[bn - 1] = '(', ')'
		return strings.Join([]string{"IN", string(b)}, " ")
	}), len(v)}
}

func NI(v []interface{}) Caller {
	return &QExpr{v, ExprFn(func()(string) {
		// make string `NOT IN (?,?,?,...)`
		bn := len(v) * 2 + 1
		b := make([]byte, bn)
		copy(b[1:], bytes.Repeat([]byte{'?',','}, len(v)))
		b[0], b[bn - 1] = '(', ')'
		return strings.Join([]string{"NOT IN", string(b)}, " ")
	}), len(v)}
}

func IsNull() Caller {
	return &QExpr{nil, " IS NULL ", 0}
}

func NotNull() Caller {
	return &QExpr{nil, " IS NOT NULL ", 0}
}

func Between(begin interface{}, end interface{}) Caller {
	var v = []interface{}{begin, end}
	return &QExpr{v, " BETWEEN ? AND ? ", 2}
}

func NotBetween(begin interface{}, end interface{}) Caller {
	var v = []interface{}{begin, end}
	return &QExpr{v, " NOT BETWEEN ? AND ? ", 2}
}

func Like(v string) Caller {
	var val interface{}
	val = fmt.Sprintf("%%%s%%", v)
	return &QExpr{val, " LIKE ?", 1}
}

func NotLike(v string) Caller {
	var val interface{}
	val = fmt.Sprintf("%%%s%%", v)
	return &QExpr{val, " NOT LIKE ?", 1}
}

type Limit []int

func (limit Limit) IsEmpty()bool  {
	return len(limit) == 0
}

func (limit Limit) Begin()int  {
	if len(limit) == 0 {
		return -1
	}
	if len(limit) == 1 {
		return 0
	}
	return limit[0]
}

func (limit Limit) MaxNum() int  {
	if len(limit) == 0 {
		return -1
	}
	if len(limit) == 1 {
		return limit[0]
	}
	return limit[1]
}

