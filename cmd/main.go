//go:build JS
// +build JS

package main

import (
	_ "crypto/sha512"
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/oarkflow/pkg/evaluate"
	"github.com/oarkflow/pkg/rule"
	"github.com/oarkflow/pkg/str"
	"github.com/oarkflow/pkg/timeutil"
)

func main() {
	evaluate.AddCustomOperator("age", builtinAge)
	evaluate.AddCustomOperator("string", toString)
	done := make(chan struct{}, 0)
	js.Global().Set("applyRule", js.FuncOf(applyRule))
	<-done
}

func applyRule(this js.Value, args []js.Value) interface{} {
	var rules []*rule.Rule
	var data map[string]any
	err := json.Unmarshal(str.ToByte(args[1].String()), &rules)
	if err != nil {
		return err.Error()
	}
	err = json.Unmarshal(str.ToByte(args[0].String()), &data)
	if err != nil {
		return err.Error()
	}
	var ds []any
	for _, r := range rules {
		d, err := r.Apply(data)
		if err != nil {
			return err.Error()
		}
		ds = append(ds, d)
	}
	return ds
}

func builtinAge(ctx evaluate.EvalContext) (interface{}, error) {
	if err := ctx.CheckArgCount(1); err != nil {
		return 0, err
	}
	left, err := ctx.Arg(0)
	if err != nil {
		return nil, err
	}
	t, err := timeutil.ParseTime(left)
	if err != nil {
		return nil, err
	}
	return timeutil.CalculateToNow(t), err
}

// ToString converts the given value to a string.
func toString(ctx evaluate.EvalContext) (interface{}, error) {
	if err := ctx.CheckArgCount(1); err != nil {
		return nil, err
	}
	left, err := ctx.Arg(0)
	if err != nil {
		return nil, err
	}
	return fmt.Sprintf("%v", left), nil
}
