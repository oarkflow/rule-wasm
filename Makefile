build:
	GOARCH=wasm GOOS=js go build -o ./examples/static/rule.wasm ./cmd/main.go
	cp "$(GOROOT)/misc/wasm/wasm_exec.js" ./examples/static/