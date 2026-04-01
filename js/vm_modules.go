package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/dop251/goja"
	gojabuffer "github.com/dop251/goja_nodejs/buffer"
	"github.com/dop251/goja_nodejs/require"
)

func registerProcessModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("process", func(rt *goja.Runtime, module *goja.Object) {
		env := map[string]string{}
		for _, entry := range os.Environ() {
			key, value, ok := strings.Cut(entry, "=")
			if !ok {
				env[key] = ""
				continue
			}
			env[key] = value
		}
		if err := module.Get("exports").ToObject(rt).Set("env", env); err != nil {
			panic(rt.NewGoError(err))
		}
		if err := module.Get("exports").ToObject(rt).Set("argv", v.currentProcessArgv()); err != nil {
			panic(rt.NewGoError(err))
		}
	})
}

func registerAssertModule(reg *require.Registry) {
	reg.RegisterNativeModule("assert", func(rt *goja.Runtime, module *goja.Object) {
		assertObj := rt.NewObject()
		assertFailure := func(call goja.FunctionCall, fallback string) {
			msg := fallback
			if arg := call.Argument(2); !goja.IsUndefined(arg) && !goja.IsNull(arg) {
				msg = arg.String()
			}
			panic(rt.NewGoError(errors.New(msg)))
		}

		_ = assertObj.Set("ok", func(call goja.FunctionCall) goja.Value {
			if !call.Argument(0).ToBoolean() {
				msg := "assert.ok failed"
				if arg := call.Argument(1); !goja.IsUndefined(arg) && !goja.IsNull(arg) {
					msg = arg.String()
				}
				panic(rt.NewGoError(errors.New(msg)))
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("strictEqual", func(call goja.FunctionCall) goja.Value {
			if !call.Argument(0).StrictEquals(call.Argument(1)) {
				assertFailure(call, "assert.strictEqual failed")
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("notStrictEqual", func(call goja.FunctionCall) goja.Value {
			if call.Argument(0).StrictEquals(call.Argument(1)) {
				assertFailure(call, "assert.notStrictEqual failed")
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("deepStrictEqual", func(call goja.FunctionCall) goja.Value {
			a := call.Argument(0).Export()
			b := call.Argument(1).Export()
			if !reflect.DeepEqual(a, b) {
				assertFailure(call, "assert.deepStrictEqual failed")
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("fail", func(call goja.FunctionCall) goja.Value {
			msg := "assert.fail called"
			if arg := call.Argument(0); !goja.IsUndefined(arg) && !goja.IsNull(arg) {
				msg = arg.String()
			}
			panic(rt.NewGoError(errors.New(msg)))
		})

		_ = assertObj.Set("throws", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(rt.NewGoError(errors.New("assert.throws expects a function")))
			}
			_, err := fn(goja.Undefined())
			if err == nil {
				panic(rt.NewGoError(errors.New("assert.throws failed")))
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("rejects", func(call goja.FunctionCall) goja.Value {
			candidate := call.Argument(0)
			if fn, ok := goja.AssertFunction(candidate); ok {
				v, err := fn(goja.Undefined())
				if err != nil {
					p, resolve, _ := rt.NewPromise()
					if rerr := resolve(goja.Undefined()); rerr != nil {
						panic(rt.NewGoError(rerr))
					}
					return rt.ToValue(p)
				}
				candidate = v
			}

			obj := candidate.ToObject(rt)
			then, ok := goja.AssertFunction(obj.Get("then"))
			if !ok {
				panic(rt.NewGoError(errors.New("assert.rejects expects a Promise or async function")))
			}

			p, resolve, reject := rt.NewPromise()
			onFulfilled := rt.ToValue(func(goja.FunctionCall) goja.Value {
				if err := reject(rt.NewGoError(errors.New("assert.rejects failed"))); err != nil {
					panic(rt.NewGoError(err))
				}
				return goja.Undefined()
			})
			onRejected := rt.ToValue(func(goja.FunctionCall) goja.Value {
				if err := resolve(goja.Undefined()); err != nil {
					panic(rt.NewGoError(err))
				}
				return goja.Undefined()
			})
			if _, err := then(candidate, onFulfilled, onRejected); err != nil {
				panic(rt.NewGoError(err))
			}
			return rt.ToValue(p)
		})

		_ = module.Set("exports", assertObj)
	})
}

func registerFSModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("fs", func(rt *goja.Runtime, module *goja.Object) {
		fsObj := rt.NewObject()

		readFileSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			data, err := os.ReadFile(path)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			enc := call.Argument(1)
			if goja.IsUndefined(enc) || goja.IsNull(enc) {
				return gojabuffer.WrapBytes(rt, data)
			}
			return gojabuffer.EncodeBytes(rt, data, enc)
		}

		toBytes := func(value goja.Value, enc goja.Value) []byte {
			return gojabuffer.DecodeBytes(rt, value, enc)
		}

		writeFileSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			data := toBytes(call.Argument(1), call.Argument(2))
			if err := os.WriteFile(path, data, 0o600); err != nil {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		appendFileSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			data := toBytes(call.Argument(1), call.Argument(2))
			f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			defer f.Close()
			if _, err := f.Write(data); err != nil {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		mkdirSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			recursive := false
			if opt := call.Argument(1); !goja.IsUndefined(opt) && !goja.IsNull(opt) {
				o := opt.ToObject(rt)
				recursive = o.Get("recursive").ToBoolean()
			}
			var err error
			if recursive {
				err = os.MkdirAll(path, 0o750)
			} else {
				err = os.Mkdir(path, 0o750)
			}
			if err != nil {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		readdirSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			entries, err := os.ReadDir(path)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			out := make([]any, 0, len(entries))
			for _, e := range entries {
				out = append(out, e.Name())
			}
			return rt.ToValue(out)
		}

		statSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			info, err := os.Stat(path)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			o := rt.NewObject()
			_ = o.Set("size", info.Size())
			_ = o.Set("mode", uint32(info.Mode()))
			_ = o.Set("mtimeMs", float64(info.ModTime().UnixNano())/1e6)
			_ = o.Set("isFile", func(goja.FunctionCall) goja.Value { return rt.ToValue(info.Mode().IsRegular()) })
			_ = o.Set("isDirectory", func(goja.FunctionCall) goja.Value { return rt.ToValue(info.IsDir()) })
			return o
		}

		rmSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			recursive := false
			force := false
			if opt := call.Argument(1); !goja.IsUndefined(opt) && !goja.IsNull(opt) {
				o := opt.ToObject(rt)
				recursive = o.Get("recursive").ToBoolean()
				force = o.Get("force").ToBoolean()
			}
			var err error
			if recursive {
				err = os.RemoveAll(path)
			} else {
				err = os.Remove(path)
			}
			if err != nil && (!force || !errors.Is(err, fs.ErrNotExist)) {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		existsSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			_, err := os.Stat(path)
			if err == nil {
				return rt.ToValue(true)
			}
			if errors.Is(err, fs.ErrNotExist) {
				return rt.ToValue(false)
			}
			panic(rt.NewGoError(err))
		}

		_ = fsObj.Set("readFileSync", readFileSync)
		_ = fsObj.Set("writeFileSync", writeFileSync)
		_ = fsObj.Set("appendFileSync", appendFileSync)
		_ = fsObj.Set("mkdirSync", mkdirSync)
		_ = fsObj.Set("readdirSync", readdirSync)
		_ = fsObj.Set("statSync", statSync)
		_ = fsObj.Set("rmSync", rmSync)
		_ = fsObj.Set("existsSync", existsSync)

		promises := rt.NewObject()
		bindPromise := func(fn func(goja.FunctionCall) goja.Value) func(goja.FunctionCall) goja.Value {
			return func(call goja.FunctionCall) goja.Value {
				p, resolve, reject := rt.NewPromise()
				go func(args []goja.Value) {
					v.schedule(func() {
						defer func() {
							if r := recover(); r != nil {
								reason := rt.NewGoError(fmt.Errorf("%v", r))
								_ = reject(reason)
							}
						}()
						res := fn(goja.FunctionCall{Arguments: args})
						_ = resolve(res)
					})
				}(append([]goja.Value(nil), call.Arguments...))
				return rt.ToValue(p)
			}
		}
		_ = promises.Set("readFile", bindPromise(readFileSync))
		_ = promises.Set("writeFile", bindPromise(writeFileSync))
		_ = promises.Set("appendFile", bindPromise(appendFileSync))
		_ = promises.Set("mkdir", bindPromise(mkdirSync))
		_ = promises.Set("readdir", bindPromise(readdirSync))
		_ = promises.Set("stat", bindPromise(statSync))
		_ = promises.Set("rm", bindPromise(rmSync))
		_ = fsObj.Set("promises", promises)

		_ = module.Set("exports", fsObj)
	})
}

func registerTimersPromisesModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("timers/promises", func(rt *goja.Runtime, module *goja.Object) {
		o := rt.NewObject()
		_ = o.Set("setTimeout", func(call goja.FunctionCall) goja.Value {
			ms := call.Argument(0).ToInteger()
			if ms < 0 {
				ms = 0
			}
			value := call.Argument(1)
			p, resolve, _ := rt.NewPromise()
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				v.schedule(func() {
					_ = resolve(value)
				})
			})
			return rt.ToValue(p)
		})
		_ = module.Set("exports", o)
	})
}

func registerDBSPTestModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("@dbsp/test", func(rt *goja.Runtime, module *goja.Object) {
		o := rt.NewObject()
		_ = o.Set("assert", require.Require(rt, "assert"))
		_ = o.Set("sleep", func(call goja.FunctionCall) goja.Value {
			setTimeout := require.Require(rt, "timers/promises").ToObject(rt).Get("setTimeout")
			fn, ok := goja.AssertFunction(setTimeout)
			if !ok {
				panic(rt.NewGoError(errors.New("timers/promises.setTimeout is not a function")))
			}
			value, err := fn(goja.Undefined(), call.Argument(0))
			if err != nil {
				panic(rt.NewGoError(err))
			}
			return value
		})
		_ = o.Set("run", func(call goja.FunctionCall) goja.Value {
			casesVal := call.Argument(0)
			runOneVal := call.Argument(1)
			casesObj := casesVal.ToObject(rt)
			lenV := casesObj.Get("length")
			if lenV == nil || goja.IsUndefined(lenV) || goja.IsNull(lenV) {
				panic(rt.NewGoError(errors.New("@dbsp/test.run expects an array of test cases")))
			}
			runOneFn, ok := goja.AssertFunction(runOneVal)
			if !ok {
				panic(rt.NewGoError(errors.New("@dbsp/test.run expects runOne function")))
			}
			p, resolve, reject := rt.NewPromise()
			go func() {
				v.schedule(func() {
					length := int(lenV.ToInteger())
					passed := 0
					failed := []string{}
					for i := 0; i < length; i++ {
						caseV := casesObj.Get(strconv.Itoa(i))
						name := fmt.Sprintf("case-%d", i)
						if n := caseV.ToObject(rt).Get("name"); n != nil && !goja.IsUndefined(n) && !goja.IsNull(n) {
							name = n.String()
						}
						_, err := runOneFn(goja.Undefined(), caseV)
						if err != nil {
							failed = append(failed, fmt.Sprintf("%s: %v", name, err))
							continue
						}
						passed++
						fmt.Printf("[PASS] %s\n", name)
					}
					fmt.Printf("\n%d/%d tests passed\n", passed, length)
					if len(failed) > 0 {
						for _, f := range failed {
							fmt.Printf("[FAIL] %s\n", f)
						}
						_ = reject(rt.NewGoError(fmt.Errorf("test run failed: %d failures", len(failed))))
						return
					}
					_ = resolve(goja.Undefined())
				})
			}()
			return rt.ToValue(p)
		})
		_ = module.Set("exports", o)
	})
}

func registerDBSPMinimistAlias(reg *require.Registry) {
	reg.RegisterNativeModule("@dbsp/minimist", func(rt *goja.Runtime, module *goja.Object) {
		_ = module.Set("exports", require.Require(rt, "minimist"))
	})
}

func registerNodeAliases(reg *require.Registry) {
	aliases := map[string]string{
		"node:assert":          "assert",
		"node:buffer":          "buffer",
		"node:console":         "console",
		"node:process":         "process",
		"node:url":             "url",
		"node:util":            "util",
		"node:fs":              "fs",
		"node:fs/promises":     "fs",
		"node:timers/promises": "timers/promises",
	}
	for alias, target := range aliases {
		aliasCopy := alias
		targetCopy := target
		reg.RegisterNativeModule(aliasCopy, func(rt *goja.Runtime, module *goja.Object) {
			if targetCopy == "fs" && aliasCopy == "node:fs/promises" {
				fsModule := require.Require(rt, "fs").ToObject(rt)
				_ = module.Set("exports", fsModule.Get("promises"))
				return
			}
			_ = module.Set("exports", require.Require(rt, targetCopy))
		})
	}
}

func installTextEncodingGlobals(rt *goja.Runtime) {
	_, err := rt.RunString(`
if (typeof TextEncoder === "undefined") {
  globalThis.TextEncoder = class TextEncoder {
    encode(input = "") { return Buffer.from(String(input), "utf8"); }
  };
}
if (typeof TextDecoder === "undefined") {
  globalThis.TextDecoder = class TextDecoder {
    constructor(label = "utf8") { this.label = String(label || "utf8"); }
    decode(input) {
      if (input == null) return "";
      if (typeof input === "string") return input;
      return Buffer.from(input).toString(this.label);
    }
  };
}
`)
	if err != nil {
		panic(rt.NewGoError(err))
	}
}
