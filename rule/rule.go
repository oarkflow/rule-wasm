package rule

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"encoding/json"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/pkg/dipper"
	"github.com/oarkflow/pkg/evaluate"
	"github.com/oarkflow/pkg/jet"
	"github.com/oarkflow/pkg/maputil"
	"github.com/oarkflow/pkg/str"
	"github.com/oarkflow/pkg/timeutil"
)

type JoinOperator string

const (
	AND JoinOperator = "AND"
	OR  JoinOperator = "OR"
	NOT JoinOperator = "NOT"
)

// Creating rule to work with data of type map[string]any

type ConditionOperator string

const (
	EQ          ConditionOperator = "eq"
	NEQ         ConditionOperator = "neq"
	GT          ConditionOperator = "gt"
	LT          ConditionOperator = "lt"
	GTE         ConditionOperator = "gte"
	LTE         ConditionOperator = "lte"
	EqCount     ConditionOperator = "eq_count"
	NeqCount    ConditionOperator = "neq_count"
	GtCount     ConditionOperator = "gt_count"
	LtCount     ConditionOperator = "lt_count"
	GteCount    ConditionOperator = "gte_count"
	LteCount    ConditionOperator = "lte_count"
	BETWEEN     ConditionOperator = "between"
	IN          ConditionOperator = "in"
	NotIn       ConditionOperator = "not_in"
	CONTAINS    ConditionOperator = "contains"
	NotContains ConditionOperator = "not_contains"
	StartsWith  ConditionOperator = "starts_with"
	EndsWith    ConditionOperator = "ends_with"
	NotZero     ConditionOperator = "not_zero"
	IsZero      ConditionOperator = "is_zero"
	IsNull      ConditionOperator = "is_null"
	NotNull     ConditionOperator = "not_null"
)

type Filter struct {
	LookupData    any        `json:"lookup_data"`
	LookupHandler func() any `json:"-"`
	Key           string     `json:"key"`
	Condition     string     `json:"condition"`
	LookupSource  string     `json:"lookup_source"`
}

type Expr struct {
	Value string `json:"value"`
}

type (
	Data      any
	Condition struct {
		Filter       Filter            `json:"filter"`
		Value        any               `json:"value"`
		Key          string            `json:"key"`
		ConditionKey string            `json:"condition_key"`
		Field        string            `json:"field"`
		Operator     ConditionOperator `json:"operator"`
	}
)

var re = regexp.MustCompile("\\[([^\\[\\]]*)\\]")

func unique(s []string) []string {
	inResult := make(map[string]bool)
	var result []string
	for _, str := range s {
		if _, ok := inResult[str]; !ok {
			inResult[str] = true
			result = append(result, str)
		}
	}
	return result
}

func (condition *Condition) filterMap(data Data) any {
	if condition.Filter.Key == "" {
		return nil
	}
	if condition.Filter.LookupData == nil && condition.Filter.LookupHandler == nil {
		return nil
	} else if condition.Filter.LookupData == nil {
		condition.Filter.LookupData = condition.Filter.LookupHandler()
	}
	if condition.Filter.LookupData != nil {
		lookupData := condition.Filter.LookupData
		if condition.Filter.Condition != "" {
			c := condition.Filter.Condition
			tags := unique(re.FindAllString(c, -1))
			for _, tag := range tags {
				if strings.Contains(tag, "[data.") {
					dField := strings.ReplaceAll(strings.ReplaceAll(tag, "[data.", ""), "]", "")
					v := dipper.Get(data, dField)
					c = strings.ReplaceAll(c, tag, fmt.Sprintf("'%v'", v))
				}
			}
			eval, err := evaluate.Parse(c)
			if err == nil {
				switch d := condition.Filter.LookupData.(type) {
				case []map[string]any:
					var filteredLookupData []map[string]any
					for _, dRow := range d {
						param := evaluate.NewEvalParams(dRow)
						rs, err := eval.Eval(param)
						if err == nil {
							if rs.(bool) {
								filteredLookupData = append(filteredLookupData, dRow)
							}
						}
					}
					lookupData = filteredLookupData
				case []any:
					var filteredLookupData []map[string]any
					for _, t := range d {
						switch dRow := t.(type) {
						case map[string]any:
							param := evaluate.NewEvalParams(dRow)
							rs, err := eval.Eval(param)
							if err == nil {
								if rs.(bool) {
									filteredLookupData = append(filteredLookupData, dRow)
								}
							}
						}
					}
					lookupData = filteredLookupData
				}
			}
		}
		lookupData = dipper.Get(lookupData, condition.Filter.Key)
		switch lookupData := lookupData.(type) {
		case []any:
			if len(lookupData) > 0 {
				d := dipper.FilterSlice(data, condition.Field, lookupData)
				if dipper.Error(d) == nil {
					if strings.Contains(condition.Field, ".[].") {
						p := strings.Split(condition.Field, ".[].")
						left := p[0]
						if left != "" {
							dipper.Set(data, left, d)
						}
					} else {
						dipper.Set(data, condition.Field, d)
					}
				}
			} else {
				if strings.Contains(condition.Field, ".[].") {
					p := strings.Split(condition.Field, ".[].")
					left := p[0]
					if left != "" {
						dipper.Set(data, left, nil)
					}
				} else {
					dipper.Set(data, condition.Field, nil)
				}
			}

		case []string:
			if len(lookupData) > 0 {
				var t []any
				for _, a := range lookupData {
					t = append(t, a)
				}
				d := dipper.FilterSlice(data, condition.Field, t)
				if dipper.Error(d) == nil {
					if strings.Contains(condition.Field, ".[].") {
						p := strings.Split(condition.Field, ".[].")
						left := p[0]
						if left != "" {
							dipper.Set(data, left, d)
						}
					} else {
						dipper.Set(data, condition.Field, d)
					}
				}
			} else {
				if strings.Contains(condition.Field, ".[].") {
					p := strings.Split(condition.Field, ".[].")
					left := p[0]
					if left != "" {
						dipper.Set(data, left, nil)
					}
				} else {
					dipper.Set(data, condition.Field, nil)
				}
			}
		}
		return lookupData
	}
	return nil
}

func (condition *Condition) checkEq(val any) bool {
	switch val := val.(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case string:
			return strings.EqualFold(val, gtVal)
		}
		return false
	case int:
		switch gtVal := condition.Value.(type) {
		case int:
			return val == gtVal
		case uint:
			return val == int(gtVal)
		case float64:
			return float64(val) == gtVal
		}
		return false
	case float64:
		switch gtVal := condition.Value.(type) {
		case int:
			return val == float64(gtVal)
		case uint:
			return val == float64(gtVal)
		case float64:
			return val == gtVal
		}
		return false
	}
	return false
}

func (condition *Condition) checkNeq(val any) bool {
	switch val := val.(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case string:
			return !strings.EqualFold(val, gtVal)
		}
		return false
	case int:
		switch gtVal := condition.Value.(type) {
		case int:
			return val != gtVal
		case float64:
			return float64(val) != gtVal
		}
		return false
	case float64:
		switch gtVal := condition.Value.(type) {
		case int:
			return val != float64(gtVal)
		case float64:
			return val != gtVal
		}
		return false
	}

	return false
}

func (condition *Condition) checkGt(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := condition.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.After(smaller)
		}
		return false
	case int:
		switch gtVal := condition.Value.(type) {
		case int:
			return val > gtVal
		case float64:
			return float64(val) > gtVal
		}
		return false
	case float64:
		switch gtVal := condition.Value.(type) {
		case int:
			return val > float64(gtVal)
		case float64:
			return val > gtVal
		}
		return false
	}

	return false
}

func (condition *Condition) checkLt(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := condition.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.Before(smaller)
		}
		return false
	case int:
		switch ltVal := condition.Value.(type) {
		case int:
			return val < ltVal
		case uint:
			return val < int(ltVal)
		case float64:
			return float64(val) < ltVal
		}
		return false
	case float64:
		switch ltVal := condition.Value.(type) {
		case int:
			return val < float64(ltVal)
		case float64:
			return val < ltVal
		}
		return false
	}

	return false
}

func (condition *Condition) checkGte(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := condition.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.After(smaller) || from.Equal(smaller)
		}
		return false
	case int:
		switch gtVal := condition.Value.(type) {
		case int:
			return val >= gtVal
		case float64:
			return float64(val) >= gtVal
		}
		return false
	case float64:
		switch gtVal := condition.Value.(type) {
		case int:
			return val >= float64(gtVal)
		case float64:
			return val >= gtVal
		}
		return false
	}
	return false
}

func (condition *Condition) checkLte(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		from, err := timeutil.ParseTime(val)
		if err != nil {
			return false
		}
		switch gtVal := condition.Value.(type) {
		case string:
			smaller, err := timeutil.ParseTime(gtVal)
			if err != nil {
				return false
			}
			return from.Before(smaller) || from.Equal(smaller)
		}
		return false
	case int:
		switch ltVal := condition.Value.(type) {
		case int:
			return val <= ltVal
		case float64:
			return float64(val) <= ltVal
		}
		return false
	case float64:
		switch ltVal := condition.Value.(type) {
		case int:
			return val <= float64(ltVal)
		case float64:
			return val <= ltVal
		}
		return false
	}

	return false
}

func (condition *Condition) checkBetween(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case []string:
			from, err := timeutil.ParseTime(val)
			if err != nil {
				return false
			}
			start, err := timeutil.ParseTime(gtVal[0])
			if err != nil {
				return false
			}
			last, err := timeutil.ParseTime(gtVal[1])
			if err != nil {
				return false
			}
			return (from.After(start) || from.Equal(start)) && (from.Before(last) || from.Equal(last))
		}
		return false
	case int:
		switch ltVal := condition.Value.(type) {
		case []int:
			return val >= ltVal[0] && val <= ltVal[1]
		case []float64:
			return float64(val) >= ltVal[0] && float64(val) <= ltVal[1]
		}
		return false
	case float64:
		switch ltVal := condition.Value.(type) {
		case []int:
			return val >= float64(ltVal[0]) && val <= float64(ltVal[1])
		case []float64:
			return val >= ltVal[0] && val <= ltVal[1]
		}
		return false
	}

	return false
}

func (condition *Condition) checkIn(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case []string:
			for _, v := range gtVal {
				if strings.EqualFold(val, v) {
					return true
				}
			}
			return false
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(val, fmt.Sprintf("%v", v)) {
					return true
				}
			}
			return false
		}
		return false
	case int:
		switch gtVal := condition.Value.(type) {
		case []int:
			for _, v := range gtVal {
				if val == v {
					return true
				}
			}
			return false
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(val), fmt.Sprintf("%v", v)) {
					return true
				}
			}
			return false
		}
		return false
	case float64:
		switch gtVal := condition.Value.(type) {
		case []float64:
			for _, v := range gtVal {
				if val == v {
					return true
				}
			}
			return false
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(int(val)), fmt.Sprintf("%v", v)) {
					return true
				}
			}
			return false
		}
		return false
	}

	return false
}

func (condition *Condition) checkNotIn(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case []string:
			for _, v := range gtVal {
				if strings.EqualFold(val, v) {
					return false
				}
			}
			return true
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(val, fmt.Sprintf("%v", v)) {
					return false
				}
			}
			return true
		}
		return false
	case int:
		switch gtVal := condition.Value.(type) {
		case []int:
			for _, v := range gtVal {
				if val == v {
					return false
				}
			}
			return true
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(val), fmt.Sprintf("%v", v)) {
					return false
				}
			}
			return true
		}
		return false
	case float64:
		switch gtVal := condition.Value.(type) {
		case []float64:
			for _, v := range gtVal {
				if val == v {
					return false
				}
			}
			return true
		case []interface{}:
			for _, v := range gtVal {
				if strings.EqualFold(strconv.Itoa(int(val)), fmt.Sprintf("%v", v)) {
					return false
				}
			}
			return true
		}
		return false
	}

	return false
}

func (condition *Condition) checkContains(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case string:
			return strings.Contains(val, gtVal)
		}
		return false
	}

	return false
}

func (condition *Condition) checkNotContains(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case string:
			return !strings.Contains(val, gtVal)
		}
		return false
	}
	return false
}

func (condition *Condition) checkStartsWith(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case string:
			return strings.HasPrefix(val, gtVal)
		}
		return false
	}
	return false
}

func (condition *Condition) checkEndsWith(data Data) bool {
	switch val := dipper.Get(data, condition.Field).(type) {
	case string:
		switch gtVal := condition.Value.(type) {
		case string:
			return strings.HasSuffix(val, gtVal)
		}
		return false
	}
	return false
}

func (condition *Condition) checkEqCount(data Data) bool {
	var d any
	t := dipper.Get(data, condition.Field)
	if dipper.Error(t) == nil {
		d = t
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := condition.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", condition.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() == gtVal && valKind.Len() != 0
}

func (condition *Condition) checkNeqCount(data Data) bool {
	var d any
	t := dipper.Get(data, condition.Field)
	if dipper.Error(t) == nil {
		d = t
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := condition.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", condition.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() != gtVal && valKind.Len() != 0
}

func (condition *Condition) checkGtCount(data Data) bool {
	var d any
	t := dipper.Get(data, condition.Field)
	if dipper.Error(t) == nil {
		d = t
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := condition.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", condition.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() > gtVal && valKind.Len() != 0
}

func (condition *Condition) checkGteCount(data Data) bool {
	var d any
	t := dipper.Get(data, condition.Field)
	if dipper.Error(t) == nil {
		d = t
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := condition.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", condition.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() >= gtVal && valKind.Len() != 0
}

func (condition *Condition) checkLtCount(data Data) bool {
	var d any
	t := dipper.Get(data, condition.Field)
	if dipper.Error(t) == nil {
		d = t
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := condition.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", condition.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() < gtVal && valKind.Len() != 0
}

func (condition *Condition) checkLteCount(data Data) bool {
	var d any
	t := dipper.Get(data, condition.Field)
	if dipper.Error(t) == nil {
		d = t
	} else {
		d = []string{}
	}
	valKind := reflect.ValueOf(d)
	if valKind.Kind() != reflect.Slice {
		if d == nil {
			return false
		}
		var dArray []any
		dArray = append(dArray, d)
		valKind = reflect.ValueOf(dArray)
	}
	var gtVal int
	switch v := condition.Value.(type) {
	case []any:
		gtVal = len(v)
	default:
		g, err := strconv.Atoi(fmt.Sprintf("%v", condition.Value))
		if err != nil {
			return false
		}
		gtVal = g
	}
	return valKind.Len() <= gtVal && valKind.Len() != 0
}

func (condition *Condition) Validate(data Data) bool {
	switch data := data.(type) {
	case map[string]any:
		val := dipper.Get(data, condition.Field)
		if val == nil {
			return false
		}

		expr := ""
		switch v := condition.Value.(type) {
		case Expr:
			expr = v.Value
		case map[string]any:
			if t, ok := v["expr"]; ok {
				switch t := t.(type) {
				case string:
					expr = t
				}
			}
		}
		if expr != "" {
			condition.Filter.Condition = expr
		}
		lookupFiltered := condition.filterMap(data)
		if condition.Filter.Condition != "" {
			condition.Value = lookupFiltered
		}
		switch condition.Operator {
		case EQ:
			return condition.checkEq(val)
		case NEQ:
			return condition.checkNeq(val)
		case GT:
			return condition.checkGt(data)
		case LT:
			return condition.checkLt(data)
		case GTE:
			return condition.checkGte(data)
		case LTE:
			return condition.checkLte(data)
		case BETWEEN:
			return condition.checkBetween(data)
		case IN:
			return condition.checkIn(data)
		case NotIn:
			return condition.checkNotIn(data)
		case CONTAINS:
			return condition.checkContains(data)
		case NotContains:
			return condition.checkNotContains(data)
		case StartsWith:
			return condition.checkStartsWith(data)
		case EndsWith:
			return condition.checkEndsWith(data)
		case IsZero:
			return reflect.ValueOf(dipper.Get(data, condition.Field)).IsZero()
		case NotZero:
			return !reflect.ValueOf(dipper.Get(data, condition.Field)).IsZero()
		case IsNull:
			return dipper.Get(data, condition.Field) == nil
		case NotNull:
			val := dipper.Get(data, condition.Field)
			if err := dipper.Error(val); err != nil {
				if err == dipper.ErrNotFound {
					return false
				}
			}
			return val != nil
		case EqCount:
			return condition.checkEqCount(data)
		case NeqCount:
			return condition.checkNeqCount(data)
		case GtCount:
			return condition.checkGtCount(data)
		case GteCount:
			return condition.checkGteCount(data)
		case LtCount:
			return condition.checkLtCount(data)
		case LteCount:
			return condition.checkLteCount(data)
		}
	}
	return false
}

func NewCondition(field string, operator ConditionOperator, value any, filter ...Filter) *Condition {
	var f Filter
	if len(filter) > 0 {
		f = filter[0]
	}
	return &Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
		Filter:   f,
	}
}

type CallbackFn func(data Data) any

type Node interface {
	Apply(d Data, callback ...CallbackFn) any
}

type Conditions struct {
	Operator  JoinOperator `json:"operator,omitempty"`
	id        string
	Condition []*Condition `json:"condition,omitempty"`
	Reverse   bool         `json:"reverse"`
}

type Response struct {
	Data      Data
	Processed bool
	Result    bool
}

func (node *Conditions) Apply(d Data) Response {
	var nodeResult bool
	switch node.Operator {
	case AND:
		nodeResult = true
		for _, condition := range node.Condition {
			nodeResult = nodeResult && condition.Validate(d)
		}
		break
	case OR:
		nodeResult = false
		for _, condition := range node.Condition {
			nodeResult = nodeResult || condition.Validate(d)
		}
		break
	}
	if node.Reverse {
		nodeResult = !nodeResult
	}
	response := Response{
		Processed: true,
		Result:    nodeResult,
	}
	if nodeResult {
		response.Data = d
	}
	return response
}

type Join struct {
	Left     *Group       `json:"left,omitempty"`
	Operator JoinOperator `json:"operator,omitempty"`
	Right    *Group       `json:"right,omitempty"`
	id       string
}

func (join *Join) Apply(d Data) Response {
	leftResponse := join.Left.Apply(d)
	rightResponse := join.Right.Apply(d)
	var joinResult bool
	switch join.Operator {
	case AND:
		joinResult = leftResponse.Result && rightResponse.Result
		break
	case OR:
		joinResult = leftResponse.Result || rightResponse.Result
		break
	}
	response := Response{
		Processed: true,
		Result:    joinResult,
	}
	if joinResult {
		response.Data = d
	}
	return response
}

type Group struct {
	Left     *Conditions  `json:"left,omitempty"`
	Operator JoinOperator `json:"operator,omitempty"`
	Right    *Conditions  `json:"right,omitempty"`
	id       string
}

func (group *Group) Apply(d Data) Response {
	resultLeft := group.Left.Apply(d)
	resultRight := group.Right.Apply(d)
	var groupResult bool
	switch group.Operator {
	case AND:
		groupResult = resultLeft.Result && resultRight.Result
		break
	case OR:
		groupResult = resultLeft.Result || resultRight.Result
		break
	}
	response := Response{
		Processed: true,
		Result:    groupResult,
	}
	if groupResult {
		response.Data = d
	}
	return response
}

type Option struct {
	ID          string `json:"id"`
	ErrorMsg    string `json:"error_msg"`
	ErrorAction string `json:"error_action"` // warning message, restrict, restrict + warning message
}

type ErrorResponse struct {
	ErrorMsg    string `json:"error_msg"`
	ErrorAction string `json:"error_action"` // warning message, restrict, restrict + warning message
}

func (e *ErrorResponse) Error() string {
	bt, _ := json.Marshal(e)
	return str.FromByte(bt)
}

type Rule struct {
	successHandler CallbackFn
	ID             string        `json:"id,omitempty"`
	ErrorMsg       string        `json:"error_msg"`
	ErrorAction    string        `json:"error_action"`
	Conditions     []*Conditions `json:"conditions"`
	Groups         []*Group      `json:"groups"`
	Joins          []*Join       `json:"joins"`
}

func New(id ...string) *Rule {
	rule := &Rule{}
	if len(id) > 0 {
		rule.ID = id[0]
	} else {
		rule.ID = xid.New().String()
	}
	return rule
}

func (r *Rule) addNode(operator JoinOperator, condition ...*Condition) *Conditions {
	node := &Conditions{
		Condition: condition,
		Operator:  operator,
		id:        xid.New().String(),
	}
	r.Conditions = append(r.Conditions, node)
	return node
}

func (r *Rule) And(condition ...*Condition) *Conditions {
	return r.addNode(AND, condition...)
}

func (r *Rule) Or(condition ...*Condition) *Conditions {
	return r.addNode(OR, condition...)
}

func (r *Rule) Not(condition ...*Condition) *Conditions {
	return r.addNode(NOT, condition...)
}

func (r *Rule) Group(left *Conditions, operator JoinOperator, right *Conditions) *Group {
	group := &Group{
		Left:     left,
		Operator: operator,
		Right:    right,
		id:       xid.New().String(),
	}
	r.Groups = append(r.Groups, group)
	return group
}

func (r *Rule) Join(left *Group, operator JoinOperator, right *Group) *Join {
	join := &Join{
		Left:     left,
		Operator: operator,
		Right:    right,
		id:       xid.New().String(),
	}
	r.Joins = append(r.Joins, join)
	return join
}

func (r *Rule) AddHandler(handler CallbackFn) {
	r.successHandler = handler
}

func (r *Rule) apply(d Data) Data {
	var result, n, g, j bool
	for i, node := range r.Conditions {
		if len(node.Condition) == 0 {
			continue
		}
		if i == 0 && node.Operator == AND {
			n = true
		} else if i == 0 && node.Operator == OR {
			n = false
		}
		response := node.Apply(d)
		switch node.Operator {
		case AND:
			n = n && response.Result
			break
		case OR:
			n = n || response.Result
			break
		}
	}
	if len(r.Groups) == 0 {
		result = n
	}
	for i, group := range r.Groups {
		if i == 0 && group.Operator == AND {
			g = true
		} else if i == 0 && group.Operator == OR {
			g = false
		}
		response := group.Apply(d)
		switch group.Operator {
		case AND:
			g = g && response.Result
			break
		case OR:
			g = g || response.Result
			break
		}
	}
	if len(r.Groups) > 0 && len(r.Joins) == 0 {
		result = g
	}
	for i, join := range r.Joins {
		if i == 0 && join.Operator == AND {
			j = true
		} else if i == 0 && join.Operator == OR {
			j = false
		}
		response := join.Apply(d)
		switch join.Operator {
		case AND:
			j = j && response.Result
			break
		case OR:
			j = j || response.Result
			break
		}
	}
	if len(r.Joins) > 0 {
		result = j
	}
	if !result {
		return nil
	}
	return d
}

func (r *Rule) Apply(d Data, callback ...CallbackFn) (any, error) {
	defaultCallbackFn := func(data Data) any {
		return data
	}

	if len(callback) > 0 {
		defaultCallbackFn = callback[0]
	}
	switch d := d.(type) {
	case map[string]any:
		dt := maputil.CopyMap(d)
		rt := r.apply(dt)
		if rt == nil && r.ErrorAction != "" {
			errorMsg, _ := jet.Parse(r.ErrorMsg, d)
			return nil, &ErrorResponse{
				ErrorMsg:    errorMsg,
				ErrorAction: r.ErrorAction,
			}
		}
		if rt != nil {
			return defaultCallbackFn(d), nil
		}
		return defaultCallbackFn(nil), nil
	case []map[string]any:
		var data []map[string]any
		for _, line := range d {
			l := maputil.CopyMap(line)
			result := r.apply(l)
			if result != nil {
				data = append(data, line)
			}
		}
		if len(data) == 0 && r.ErrorAction != "" {
			errorMsg, _ := jet.Parse(r.ErrorMsg, d)
			return nil, &ErrorResponse{
				ErrorMsg:    errorMsg,
				ErrorAction: r.ErrorAction,
			}
		}
		return data, nil
	case []any:
		var data []map[string]any
		for _, line := range d {
			switch line := line.(type) {
			case map[string]any:
				l := maputil.CopyMap(line)
				result := r.apply(l)
				if result != nil {
					data = append(data, line)
				}
			}
		}
		if len(data) == 0 && r.ErrorAction != "" {
			errorMsg, _ := jet.Parse(r.ErrorMsg, d)
			return nil, &ErrorResponse{
				ErrorMsg:    errorMsg,
				ErrorAction: r.ErrorAction,
			}
		}
		return data, nil
	}
	return nil, nil
}

type Priority int

const (
	HighestPriority Priority = 1
	LowestPriority  Priority = 0
)

type PriorityRule struct {
	Rule     *Rule
	Priority int
}

type Config struct {
	Rules    []*PriorityRule
	Priority Priority
}

type GroupRule struct {
	mu     *sync.RWMutex
	Key    string          `json:"key,omitempty"`
	Rules  []*PriorityRule `json:"rules,omitempty"`
	config Config
}

func NewRuleGroup(config ...Config) *GroupRule {
	cfg := Config{}
	if len(config) > 0 {
		cfg = config[0]
	}
	return &GroupRule{
		Rules:  cfg.Rules,
		config: cfg,
		mu:     &sync.RWMutex{},
	}
}

func (r *GroupRule) AddRule(rule *Rule, priority int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Rules = append(r.Rules, &PriorityRule{
		Rule:     rule,
		Priority: priority,
	})
}

func (r *GroupRule) ApplyHighestPriority(data Data, fn ...CallbackFn) (any, error) {
	return r.apply(r.sortByPriority("DESC"), data, fn...)
}

func (r *GroupRule) ApplyLowestPriority(data Data, fn ...CallbackFn) (any, error) {
	return r.apply(r.sortByPriority(), data, fn...)
}

func (r *GroupRule) Apply(data Data, fn ...CallbackFn) (any, error) {
	if r.config.Priority == HighestPriority {
		return r.ApplyHighestPriority(data, fn...)
	}
	return r.ApplyLowestPriority(data, fn...)
}

func (r *GroupRule) apply(sortedRules []*Rule, data Data, fn ...CallbackFn) (any, error) {
	for _, rule := range sortedRules {
		response, err := rule.Apply(data, fn...)
		if response != nil {
			return response, err
		}
	}
	return nil, nil
}

func (r *GroupRule) SortByPriority(direction ...string) []*Rule {
	return r.sortByPriority(direction...)
}

func (r *GroupRule) sortByPriority(direction ...string) []*Rule {
	dir := "ASC"
	if len(direction) > 0 {
		dir = direction[0]
	}
	if dir == "DESC" {
		sort.Sort(sort.Reverse(byPriority(r.Rules)))
	} else {
		sort.Sort(byPriority(r.Rules))
	}
	res := make([]*Rule, 0, len(r.Rules))
	for _, q := range r.Rules {
		res = append(res, q.Rule)
	}
	return res
}

type byPriority []*PriorityRule

func (x byPriority) Len() int           { return len(x) }
func (x byPriority) Less(i, j int) bool { return x[i].Priority < x[j].Priority }
func (x byPriority) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
