build:
	GOARCH=wasm GOOS=js go build -o ./examples/static/rule.wasm ./cmd/main.go
	cp "$(GOROOT)/misc/wasm/wasm_exec.js" ./examples/static/

build-go:
	tinygo build -o ./examples/static/rule.wasm -target=wasm -gc=leaking -no-debug ./cmd/main.go
	cp $(tinygo env TINYGOROOT)/targets/wasm_exec.js ./examples/static/

cp-exec:
	echo $(TINYGOROOT)