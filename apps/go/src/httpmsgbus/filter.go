/***************************************************************************
 *   Copyright (C) by GFZ Potsdam                                          *
 *                                                                         *
 *   Author:  Andres Heinloo                                               *
 *   Email:   andres@gfz-potsdam.de                                        *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2, or (at your option)   *
 *   any later version. For more information, see http://www.gnu.org/      *
 ***************************************************************************/

package main

import (
	"encoding/json"
	"errors"
	"strings"
)

// Implement subset of MongoDB query language
// http://docs.mongodb.org/manual/reference/operator/query/
//
// The grammar of a mongo expression (JSON) looks like this:
//
//  <mongo_expr> = { $op_logic: [ <mongo_expr>, ... ] | $not: <mongo_expr> | <field>: <field_expr>, ... }
//  <field_expr> = { $op_field: <value>, ... } | <value>
//  $op_logic    = $and | $or | $nor
//  $op_field    = $eq | $ne | $gt | $lt | ...

// Expr is any filter expression.
type Expr interface {
	// Eval returns true if the message passes the filter.
	// The second argument is a scratch buffer.
	Eval(*Message, []interface{}) bool
}

// Value is any value.
type Value interface {
	// IsNull returns true if the value is null.
	IsNull() bool

	// List casts the value to list and returns
	// true if the cast was successful.
	List() ([]Value, bool)

	// Bool casts the value to bool and returns
	// true if the cast was successful.
	Bool() (bool, bool)

	// String casts the value to string and returns
	// true if the cast was successful.
	String() (string, bool)

	// Number casts the value to number and returns
	// true if the cast was successful.
	Number() (float64, bool)
}

// nullValue implements a Value representing a null value.
type nullValue struct {
}

func (self nullValue) IsNull() bool            { return true }
func (self nullValue) List() ([]Value, bool)   { return []Value{}, false }
func (self nullValue) Bool() (bool, bool)      { return false, false }
func (self nullValue) String() (string, bool)  { return "", false }
func (self nullValue) Number() (float64, bool) { return 0, false }

// listValue implements a Value representing a list.
type listValue struct {
	v []Value
}

func (self listValue) IsNull() bool            { return false }
func (self listValue) List() ([]Value, bool)   { return self.v, true }
func (self listValue) Bool() (bool, bool)      { return false, false }
func (self listValue) String() (string, bool)  { return "", false }
func (self listValue) Number() (float64, bool) { return 0, false }

// boolValue implements a Value representing a bool.
type boolValue struct {
	v bool
}

func (self boolValue) IsNull() bool            { return false }
func (self boolValue) List() ([]Value, bool)   { return []Value{}, false }
func (self boolValue) Bool() (bool, bool)      { return self.v, true }
func (self boolValue) String() (string, bool)  { return "", false }
func (self boolValue) Number() (float64, bool) { return 0, false }

// stringValue implements a Value representing a string.
type stringValue struct {
	v string
}

func (self stringValue) IsNull() bool            { return false }
func (self stringValue) List() ([]Value, bool)   { return []Value{}, false }
func (self stringValue) Bool() (bool, bool)      { return false, false }
func (self stringValue) String() (string, bool)  { return self.v, true }
func (self stringValue) Number() (float64, bool) { return 0, false }

// numValue implements a Value representing number.
type numValue struct {
	v float64
}

func (self numValue) IsNull() bool            { return false }
func (self numValue) List() ([]Value, bool)   { return []Value{}, false }
func (self numValue) Bool() (bool, bool)      { return false, false }
func (self numValue) String() (string, bool)  { return "", false }
func (self numValue) Number() (float64, bool) { return self.v, true }

// makeValue creates a new Value object.
func makeValue(src interface{}) (Value, error) {
	if src == nil {
		return nullValue{}, nil
	}

	switch src.(type) {
	case []interface{}:
		list := make([]Value, 0, len(src.([]interface{})))

		for _, v := range src.([]interface{}) {
			if value, err := makeValue(v); err != nil {
				return nullValue{}, err

			} else {
				list = append(list, value)
			}
		}

		return listValue{list}, nil

	case bool:
		return boolValue{src.(bool)}, nil

	case string:
		return stringValue{src.(string)}, nil

	case int:
		return numValue{float64(src.(int))}, nil

	case int32:
		return numValue{float64(src.(int32))}, nil

	case int64:
		return numValue{float64(src.(int64))}, nil

	case float32:
		return numValue{float64(src.(float32))}, nil

	case float64:
		return numValue{src.(float64)}, nil

	default:
		s, _ := json.Marshal(src)
		return nil, errors.New("unexpected value: " + string(s))
	}
}

// _extract is a helper function to extract a scoped field from BSON payload.
func _extract(d interface{}, f []string, buf []interface{}) []interface{} {
	if d, ok := d.(map[string]interface{}); !ok {
		// d is not a BSON document, stop the recursion.
		return buf

	} else if v, ok := d[f[0]]; !ok {
		// d does not contain the field, stop the recursion.
		return buf

	} else if len(f) == 1 {
		// Last element of the field name reached. Add value(s) to buf
		// and stop the recursion.
		if vlist, ok := v.([]interface{}); ok {
			// If the value is a list, return all elements.
			for _, v := range vlist {
				buf = append(buf, v)
			}

		} else {
			// Just return a single value.
			buf = append(buf, v)
		}

	} else {
		// Recurse into the next scope.
		if vlist, ok := v.([]interface{}); ok {
			// If we got a list of documents, extract matching
			// values from all of them.
			for _, v := range vlist {
				buf = _extract(v, f[1:], buf)
			}

		} else {
			// Just recurse into a single BSON document.
			buf = _extract(v, f[1:], buf)
		}
	}

	return buf
}

// extract extracts a scoped field from a message. A single field can
// have multiple values if lists are used, so a list of values is returned.
func extract(m *Message, field string, buf []interface{}) []interface{} {
	f := strings.Split(field, ".")

	if len(f) == 1 {
		switch f[0] {
		case "type":
			buf = append(buf, m.Type)
		case "sender":
			buf = append(buf, m.Sender)
		}

	} else if f[0] == "data" {
		buf = _extract(m.Data.Data, f[1:], buf)
	}

	return buf
}

// _AND implements an Expr representing the $and operator.
type _AND struct {
	exprlist []Expr
}

// Eval returns true if all operands are true.
func (self _AND) Eval(m *Message, buf []interface{}) bool {
	for _, expr := range self.exprlist {
		if !expr.Eval(m, buf) {
			return false
		}
	}

	return true
}

// _OR implements an Expr representing the $or operator.
type _OR struct {
	exprlist []Expr
}

// Eval returns true if at least one operand is true.
func (self _OR) Eval(m *Message, buf []interface{}) bool {
	for _, expr := range self.exprlist {
		if expr.Eval(m, buf) {
			return true
		}
	}

	return false
}

// _NOR implements an Expr representing the $nor operator.
type _NOR struct {
	exprlist []Expr
}

// Eval returns true if all operands are false.
func (self _NOR) Eval(m *Message, buf []interface{}) bool {
	for _, expr := range self.exprlist {
		if expr.Eval(m, buf) {
			return false
		}
	}

	return true
}

// _NOT implements an Expr representing the $not operator.
type _NOT struct {
	expr Expr
}

// Eval returns true if the operand is false.
func (self _NOT) Eval(m *Message, buf []interface{}) bool {
	return !self.expr.Eval(m, buf)
}

// _EQ implements an Expr representing the $eq operator.
type _EQ struct {
	field string
	v     Value
}

// Eval returns true if any value of the given field is equal to the given
// value.
func (self _EQ) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		if d == nil && self.v.IsNull() {
			return true
		}

		switch d.(type) {
		case bool:
			if v, ok := self.v.Bool(); ok && d.(bool) == v {
				return true
			}

		case string:
			if v, ok := self.v.String(); ok && d.(string) == v {
				return true
			}

		case int:
			if v, ok := self.v.Number(); ok && float64(d.(int)) == v {
				return true
			}

		case int32:
			if v, ok := self.v.Number(); ok && float64(d.(int32)) == v {
				return true
			}

		case int64:
			if v, ok := self.v.Number(); ok && float64(d.(int64)) == v {
				return true
			}

		case float32:
			if v, ok := self.v.Number(); ok && float64(d.(float32)) == v {
				return true
			}

		case float64:
			if v, ok := self.v.Number(); ok && d.(float64) == v {
				return true
			}
		}
	}

	return false
}

// _NE implements an Expr representing the $ne operator.
type _NE struct {
	field string
	v     Value
}

// Eval returns true if none of the values of the given field are equal to the
// given value.
func (self _NE) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		if d == nil && self.v.IsNull() {
			return false
		}

		switch d.(type) {
		case bool:
			if v, ok := self.v.Bool(); ok && d.(bool) == v {
				return false
			}

		case string:
			if v, ok := self.v.String(); ok && d.(string) == v {
				return false
			}

		case int:
			if v, ok := self.v.Number(); ok && float64(d.(int)) == v {
				return false
			}

		case int32:
			if v, ok := self.v.Number(); ok && float64(d.(int32)) == v {
				return false
			}

		case int64:
			if v, ok := self.v.Number(); ok && float64(d.(int64)) == v {
				return false
			}

		case float32:
			if v, ok := self.v.Number(); ok && float64(d.(float32)) == v {
				return false
			}

		case float64:
			if v, ok := self.v.Number(); ok && d.(float64) == v {
				return false
			}
		}
	}

	return true
}

// _GT implements an Expr representing the $gt operator.
type _GT struct {
	field string
	v     Value
}

// Eval returns true if any value of the given field is greater than the given
// value.
func (self _GT) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		switch d.(type) {
		case string:
			if v, ok := self.v.String(); ok && d.(string) > v {
				return true
			}

		case int:
			if v, ok := self.v.Number(); ok && float64(d.(int)) > v {
				return true
			}

		case int32:
			if v, ok := self.v.Number(); ok && float64(d.(int32)) > v {
				return true
			}

		case int64:
			if v, ok := self.v.Number(); ok && float64(d.(int64)) > v {
				return true
			}

		case float32:
			if v, ok := self.v.Number(); ok && float64(d.(float32)) > v {
				return true
			}

		case float64:
			if v, ok := self.v.Number(); ok && d.(float64) > v {
				return true
			}
		}
	}

	return false
}

// _GTE implements an Expr representing the $gte operator.
type _GTE struct {
	field string
	v     Value
}

// Eval returns true if any value of the given field is greater than or equal
// to the given value.
func (self _GTE) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		switch d.(type) {
		case string:
			if v, ok := self.v.String(); ok && d.(string) >= v {
				return true
			}

		case int:
			if v, ok := self.v.Number(); ok && float64(d.(int)) >= v {
				return true
			}

		case int32:
			if v, ok := self.v.Number(); ok && float64(d.(int32)) >= v {
				return true
			}

		case int64:
			if v, ok := self.v.Number(); ok && float64(d.(int64)) >= v {
				return true
			}

		case float32:
			if v, ok := self.v.Number(); ok && float64(d.(float32)) >= v {
				return true
			}

		case float64:
			if v, ok := self.v.Number(); ok && d.(float64) >= v {
				return true
			}
		}
	}

	return false
}

// _LT implements an Expr representing the $lt operator.
type _LT struct {
	field string
	v     Value
}

// Eval returns true if any value of the given field is less than the given
// value.
func (self _LT) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		switch d.(type) {
		case string:
			if v, ok := self.v.String(); ok && d.(string) < v {
				return true
			}

		case int:
			if v, ok := self.v.Number(); ok && float64(d.(int)) < v {
				return true
			}

		case int32:
			if v, ok := self.v.Number(); ok && float64(d.(int32)) < v {
				return true
			}

		case int64:
			if v, ok := self.v.Number(); ok && float64(d.(int64)) < v {
				return true
			}

		case float32:
			if v, ok := self.v.Number(); ok && float64(d.(float32)) < v {
				return true
			}

		case float64:
			if v, ok := self.v.Number(); ok && d.(float64) < v {
				return true
			}
		}
	}

	return false
}

// _LTE implements an Expr representing the $lte operator.
type _LTE struct {
	field string
	v     Value
}

// Eval returns true if any value of the given field is less than or equal to
// the given value.
func (self _LTE) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		switch d.(type) {
		case string:
			if v, ok := self.v.String(); ok && d.(string) <= v {
				return true
			}

		case int:
			if v, ok := self.v.Number(); ok && float64(d.(int)) <= v {
				return true
			}

		case int32:
			if v, ok := self.v.Number(); ok && float64(d.(int32)) <= v {
				return true
			}

		case int64:
			if v, ok := self.v.Number(); ok && float64(d.(int64)) <= v {
				return true
			}

		case float32:
			if v, ok := self.v.Number(); ok && float64(d.(float32)) <= v {
				return true
			}

		case float64:
			if v, ok := self.v.Number(); ok && d.(float64) <= v {
				return true
			}
		}
	}

	return false
}

// _IN implements an Expr representing the $in operator.
type _IN struct {
	field string
	vlist []Value
}

// Eval returns true if any value of the given field belongs to the given set
// of values.
func (self _IN) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		for _, v := range self.vlist {
			if d == nil && v.IsNull() {
				return true
			}

			switch d.(type) {
			case bool:
				if v, ok := v.Bool(); ok && d.(bool) == v {
					return true
				}

			case string:
				if v, ok := v.String(); ok && d.(string) == v {
					return true
				}

			case int:
				if v, ok := v.Number(); ok && float64(d.(int)) == v {
					return true
				}

			case int32:
				if v, ok := v.Number(); ok && float64(d.(int32)) == v {
					return true
				}

			case int64:
				if v, ok := v.Number(); ok && float64(d.(int64)) == v {
					return true
				}

			case float32:
				if v, ok := v.Number(); ok && float64(d.(float32)) == v {
					return true
				}

			case float64:
				if v, ok := v.Number(); ok && d.(float64) == v {
					return true
				}
			}
		}
	}

	return false
}

// _NIN implements an Expr representing the $nin operator.
type _NIN struct {
	field string
	vlist []Value
}

// Eval returns true if none of the values of the given field belong to the given set
// of values.
func (self _NIN) Eval(m *Message, buf []interface{}) bool {
	for _, d := range extract(m, self.field, buf) {
		for _, v := range self.vlist {
			if d == nil && v.IsNull() {
				return true
			}

			switch d.(type) {
			case bool:
				if v, ok := v.Bool(); ok && d.(bool) == v {
					return true
				}

			case string:
				if v, ok := v.String(); ok && d.(string) == v {
					return true
				}

			case int:
				if v, ok := v.Number(); ok && float64(d.(int)) == v {
					return true
				}

			case int32:
				if v, ok := v.Number(); ok && float64(d.(int32)) == v {
					return true
				}

			case int64:
				if v, ok := v.Number(); ok && float64(d.(int64)) == v {
					return true
				}

			case float32:
				if v, ok := v.Number(); ok && float64(d.(float32)) == v {
					return true
				}

			case float64:
				if v, ok := v.Number(); ok && d.(float64) == v {
					return true
				}
			}
		}
	}

	return true
}

// _EXISTS implements an Expr representing the $exists operator.
type _EXISTS struct {
	field string
	v     bool
}

// Eval returns true if the given field exists (value is true) of if the
// given field does not exist (value is false).
func (self _EXISTS) Eval(m *Message, buf []interface{}) bool {
	for _ = range extract(m, self.field, buf) {
		return self.v == true
	}

	return self.v == false
}

// makeMongoExprList creates a list of mongo expressions from a list of BSON
// documents.
func makeMongoExprList(src []interface{}) ([]Expr, error) {
	exprlist := make([]Expr, 0, len(src))

	for _, v := range src {
		if e, ok := v.(map[string]interface{}); !ok {
			s, _ := json.Marshal(v)
			return nil, errors.New("unexpected expression: " + string(s))

		} else if expr, err := makeMongoExpr(e); err != nil {
			return nil, err

		} else {
			exprlist = append(exprlist, expr)
		}
	}

	return exprlist, nil
}

// makeMongoExpr creates an Expr representing a mongo expression from a BSON
// document.
func makeMongoExpr(src map[string]interface{}) (Expr, error) {
	if len(src) > 1 {
		exprlist := make([]Expr, 0, len(src))

		for k, v := range src {
			if expr, err := makeMongoExpr(map[string]interface{}{k: v}); err != nil {
				return nil, err

			} else {
				exprlist = append(exprlist, expr)
			}
		}

		return _AND{exprlist}, nil

	} else {
		for k, v := range src {
			switch k {
			case "$and":
				if v, ok := v.([]interface{}); !ok {
					return nil, errors.New("$and requires a list")

				} else if exprlist, err := makeMongoExprList(v); err != nil {
					return nil, err

				} else {
					return _AND{exprlist}, nil
				}

			case "$or":
				if v, ok := v.([]interface{}); !ok {
					return nil, errors.New("$or requires a list")

				} else if exprlist, err := makeMongoExprList(v); err != nil {
					return nil, err

				} else {
					return _OR{exprlist}, nil
				}

			case "$nor":
				if v, ok := v.([]interface{}); !ok {
					return nil, errors.New("$nor requires a list")

				} else if exprlist, err := makeMongoExprList(v); err != nil {
					return nil, err

				} else {
					return _NOR{exprlist}, nil
				}

			case "$not":
				if v, ok := v.(map[string]interface{}); !ok {
					return nil, errors.New("$not requires a dict")

				} else if expr, err := makeMongoExpr(v); err != nil {
					return nil, err

				} else {
					return _NOT{expr}, nil
				}

			default:
				return makeFieldExpr(k, v)
			}
		}
	}

	return nil, errors.New("invalid filter expression")
}

// makeFieldExpr creates an Expr representing a field expression.
func makeFieldExpr(field string, valueSrc interface{}) (Expr, error) {
	if src, ok := valueSrc.(map[string]interface{}); !ok {
		if value, err := makeValue(valueSrc); err != nil {
			return nil, err

		} else {
			return _EQ{field, value}, nil
		}

	} else if len(src) > 1 {
		exprlist := make([]Expr, 0, len(src))

		for k, v := range src {
			if expr, err := makeFieldExpr(field, map[string]interface{}{k: v}); err != nil {
				return nil, err

			} else {
				exprlist = append(exprlist, expr)
			}
		}

		return _AND{exprlist}, nil

	} else {
		for k, v := range src {
			if value, err := makeValue(v); err != nil {
				return nil, err

			} else {
				switch k {
				case "$eq":
					if _, ok := value.List(); ok {
						return nil, errors.New("$eq does not support list")

					} else {
						return _EQ{field, value}, nil
					}

				case "$ne":
					if _, ok := value.List(); ok {
						return nil, errors.New("$ne does not support list")

					} else {
						return _NE{field, value}, nil
					}

				case "$gt":
					if _, ok := value.Number(); ok {
						return _GT{field, value}, nil

					} else if _, ok := value.String(); ok {
						return _GT{field, value}, nil

					} else {
						return nil, errors.New("$gt expects numeric or string argument")
					}

				case "$gte":
					if _, ok := value.Number(); ok {
						return _GTE{field, value}, nil

					} else if _, ok := value.String(); ok {
						return _GTE{field, value}, nil

					} else {
						return nil, errors.New("$gte expects numeric or string argument")
					}

				case "$lt":
					if _, ok := value.Number(); ok {
						return _LT{field, value}, nil

					} else if _, ok := value.String(); ok {
						return _LT{field, value}, nil

					} else {
						return nil, errors.New("$lt expects numeric or string argument")
					}

				case "$lte":
					if _, ok := value.Number(); ok {
						return _LTE{field, value}, nil

					} else if _, ok := value.String(); ok {
						return _LTE{field, value}, nil

					} else {
						return nil, errors.New("$lte expects numeric or string argument")
					}

				case "$in":
					if vlist, ok := value.List(); ok {
						return _IN{field, vlist}, nil

					} else {
						return nil, errors.New("$in expects list argument")
					}

				case "$nin":
					if vlist, ok := value.List(); ok {
						return _NIN{field, vlist}, nil

					} else {
						return nil, errors.New("$nin expects list argument")
					}

				case "$exists":
					if b, ok := value.Bool(); ok {
						return _EXISTS{field, b}, nil

					} else {
						return nil, errors.New("$exists expects boolean argument")
					}

				default:
					return nil, errors.New("unknown operator: " + k)
				}
			}
		}
	}

	return nil, errors.New("invalid filter expression")
}

// Filter represents an HMB filter.
type Filter struct {
	src  map[string]interface{}
	expr Expr
	buf  []interface{}
}

// NewFilter creates a new Filter object.
func NewFilter(src map[string]interface{}) (*Filter, error) {
	if expr, err := makeMongoExpr(src); err != nil {
		return nil, err

	} else {
		return &Filter{src, expr, []interface{}{}}, nil
	}
}

// Source returns the source BSON document.
func (self *Filter) Source() map[string]interface{} {
	return self.src
}

// Match applies the filter to a message.
func (self *Filter) Match(m *Message) bool {
	return self.expr.Eval(m, self.buf)
}
