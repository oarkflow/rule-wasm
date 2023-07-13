package main

import (
	"crypto"
	_ "crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"github.com/oarkflow/pkg/evaluate"
	"github.com/oarkflow/pkg/str"
	"github.com/oarkflow/pkg/timeutil"
	"github.com/oarkflow/rule-wasm/rule"
	"syscall/js"
)

func main() {
	evaluate.AddCustomOperator("age", builtinAge)
	done := make(chan struct{}, 0)
	js.Global().Set("wasmHash", js.FuncOf(hash))
	js.Global().Set("applyRule", js.FuncOf(applyRule))
	<-done
}

func hash(this js.Value, args []js.Value) interface{} {
	h := crypto.SHA512.New()
	h.Write([]byte(args[0].String()))

	return hex.EncodeToString(h.Sum(nil))
}

func applyRule(this js.Value, args []js.Value) interface{} {
	var rules []*rule.Rule
	var data map[string]any
	err := json.Unmarshal(str.ToByte(args[1].String()), &rules)
	if err != nil {
		return err
	}
	err = json.Unmarshal(str.ToByte(args[0].String()), &data)
	if err != nil {
		return err
	}
	var ds []any
	for _, r := range rules {
		d, err := r.Apply(data)
		if err != nil {
			return err
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
