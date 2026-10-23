package js

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
	gojabuffer "github.com/dop251/goja_nodejs/buffer"
	"github.com/dop251/goja_nodejs/require"
)

// httpClients holds the two HTTP clients of a VM, built on first use: one
// verifying TLS certificates and one skipping verification.
type httpClients struct {
	mu       sync.Mutex
	verify   *http.Client
	insecure *http.Client
}

// get returns the client for the requested TLS mode, building it on first
// use. The first insecure client logs a warning.
func (c *httpClients) get(insecure bool, v *VM) *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !insecure {
		if c.verify == nil {
			c.verify = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
		}
		return c.verify
	}
	if c.insecure == nil {
		v.logger.Info("WARNING: fetch: TLS certificate verification is disabled")
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // Explicitly requested by the script.
		c.insecure = &http.Client{Transport: t}
	}
	return c.insecure
}

// closeIdle drops the idle keep-alive connections of both clients.
func (c *httpClients) closeIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cl := range []*http.Client{c.verify, c.insecure} {
		if cl != nil {
			cl.CloseIdleConnections()
		}
	}
}

// headerPairs flattens a Go header into [name, value] pairs, one per value,
// with lower-cased names in sorted order.
func headerPairs(h http.Header) []any {
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	sort.Strings(names)
	pairs := make([]any, 0, len(names))
	for _, name := range names {
		for _, value := range h[name] {
			pairs = append(pairs, []any{strings.ToLower(name), value})
		}
	}
	return pairs
}

// exportHeaderPairs reads the [name, value] pairs of a JS headers argument;
// undefined and null are no headers.
func exportHeaderPairs(rt *goja.Runtime, value goja.Value) ([][2]string, error) {
	var pairs [][2]string
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return nil, nil
	}
	if err := rt.ExportTo(value, &pairs); err != nil {
		return nil, fmt.Errorf("headers must be an array of [name, value] pairs")
	}
	return pairs, nil
}

// exportBody reads a JS body argument (a string, an ArrayBuffer, or a
// Uint8Array); undefined and null are no body.
func exportBody(rt *goja.Runtime, value goja.Value) []byte {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return nil
	}
	return gojabuffer.DecodeBytes(rt, value, goja.Undefined())
}

// registerHTTPModule registers the native transport behind the http stdlib
// module as "@dbsp/http". It holds no fetch or serve semantics:
//
//   - request({method, url, headers, body, insecure}) returns {promise,
//     cancel}: the promise resolves to {status, statusText, url, headers,
//     body} and rejects with TypeError("fetch failed") on a transport
//     error, carrying the Go error as cause. An HTTP error status resolves.
//   - listen({hostname, port, cert, key}, handler) starts a server, HTTPS
//     when the PEM cert and key are given, and returns {hostname, port,
//     shutdown}. Each request calls handler({method, url, headers, body,
//     remoteAddr}, respond) on the event loop, and respond({status,
//     headers, body}) answers it. shutdown() returns a Promise that resolves
//     once the in-flight requests are answered.
//
// Headers travel as [name, value] pairs and bodies as ArrayBuffers. Both
// run under the VM context, so closing the VM aborts every request in
// flight and closes every server.
func registerHTTPModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("@dbsp/http", func(rt *goja.Runtime, module *goja.Object) {
		exports := module.Get("exports").ToObject(rt)
		_ = exports.Set("request", func(call goja.FunctionCall) goja.Value {
			opts := call.Argument(0).ToObject(rt)

			method := "GET"
			if m := opts.Get("method"); m != nil && !goja.IsUndefined(m) && !goja.IsNull(m) {
				method = m.String()
			}
			var body io.Reader
			if b := exportBody(rt, opts.Get("body")); b != nil {
				body = bytes.NewReader(b)
			}
			headers, err := exportHeaderPairs(rt, opts.Get("headers"))
			if err != nil {
				panic(rt.NewTypeError(err.Error()))
			}

			ctx, cancel := context.WithCancel(v.ctx)
			req, err := http.NewRequestWithContext(ctx, method, opts.Get("url").String(), body)
			if err != nil {
				cancel()
				panic(rt.NewTypeError(err.Error()))
			}
			for _, p := range headers {
				if strings.EqualFold(p[0], "host") {
					req.Host = p[1]
					continue
				}
				req.Header.Add(p[0], p[1])
			}
			client := v.httpClients.get(opts.Get("insecure").ToBoolean(), v)

			promise, resolve, reject := rt.NewPromise()
			go func() {
				defer cancel()
				resp, err := client.Do(req)
				var data []byte
				if err == nil {
					data, err = io.ReadAll(resp.Body)
					resp.Body.Close()
				}
				v.schedule(func() {
					if err != nil {
						reason := rt.NewTypeError("fetch failed")
						_ = reason.Set("cause", rt.NewGoError(err))
						_ = reject(reason)
						return
					}
					res := rt.NewObject()
					_ = res.Set("status", resp.StatusCode)
					_ = res.Set("statusText", strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode)+" "))
					_ = res.Set("url", resp.Request.URL.String())
					_ = res.Set("headers", headerPairs(resp.Header))
					_ = res.Set("body", rt.NewArrayBuffer(data))
					_ = resolve(res)
				})
			}()

			handle := rt.NewObject()
			_ = handle.Set("promise", promise)
			_ = handle.Set("cancel", func(goja.FunctionCall) goja.Value {
				cancel()
				return goja.Undefined()
			})
			return handle
		})

		_ = exports.Set("listen", func(call goja.FunctionCall) goja.Value {
			opts := call.Argument(0).ToObject(rt)
			handler, ok := goja.AssertFunction(call.Argument(1))
			if !ok {
				panic(rt.NewTypeError("listen: handler must be a function"))
			}

			ln, err := net.Listen("tcp", net.JoinHostPort(opts.Get("hostname").String(), opts.Get("port").String()))
			if err != nil {
				panic(rt.NewGoError(err))
			}
			scheme := "http"
			if cert, key := opts.Get("cert"), opts.Get("key"); cert != nil && !goja.IsUndefined(cert) && key != nil && !goja.IsUndefined(key) {
				pair, err := tls.X509KeyPair([]byte(cert.String()), []byte(key.String()))
				if err != nil {
					ln.Close()
					panic(rt.NewGoError(fmt.Errorf("listen: %w", err)))
				}
				ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})
				scheme = "https"
			}

			type response struct {
				status  int
				headers [][2]string
				body    []byte
			}
			srv := &http.Server{
				BaseContext:       func(net.Listener) context.Context { return v.ctx },
				ReadHeaderTimeout: 30 * time.Second,
				ErrorLog:          log.New(io.Discard, "", 0),
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, "Bad Request", http.StatusBadRequest)
						return
					}
					done := make(chan response, 1)
					v.schedule(func() {
						var once sync.Once
						respond := func(call goja.FunctionCall) goja.Value {
							res := call.Argument(0).ToObject(rt)
							headers, err := exportHeaderPairs(rt, res.Get("headers"))
							if err != nil {
								panic(rt.NewTypeError(err.Error()))
							}
							out := response{status: int(res.Get("status").ToInteger()), headers: headers, body: exportBody(rt, res.Get("body"))}
							once.Do(func() { done <- out })
							return goja.Undefined()
						}
						host, port, _ := net.SplitHostPort(r.RemoteAddr)
						req := rt.NewObject()
						_ = req.Set("method", r.Method)
						_ = req.Set("url", scheme+"://"+r.Host+r.URL.RequestURI())
						// net/http moves the Host header to r.Host.
						header := r.Header.Clone()
						header.Set("Host", r.Host)
						_ = req.Set("headers", headerPairs(header))
						_ = req.Set("body", rt.NewArrayBuffer(body))
						_ = req.Set("remoteAddr", map[string]any{"hostname": host, "port": port, "transport": "tcp"})
						if _, err := handler(goja.Undefined(), req, rt.ToValue(respond)); err != nil {
							v.logger.Error(err, "http handler failed")
							once.Do(func() { done <- response{status: http.StatusInternalServerError} })
						}
					})
					select {
					case res := <-done:
						for _, p := range res.headers {
							w.Header().Add(p[0], p[1])
						}
						w.WriteHeader(res.status)
						_, _ = w.Write(res.body)
					case <-r.Context().Done():
					}
				}),
			}

			stopped := make(chan struct{})
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					v.logger.Error(err, "http server failed", "addr", ln.Addr().String())
				}
			}()
			go func() {
				select {
				case <-v.ctx.Done():
					srv.Close()
				case <-stopped:
				}
			}()

			tcpAddr := ln.Addr().(*net.TCPAddr)
			handle := rt.NewObject()
			_ = handle.Set("hostname", tcpAddr.IP.String())
			_ = handle.Set("port", tcpAddr.Port)
			var stopOnce sync.Once
			_ = handle.Set("shutdown", func(goja.FunctionCall) goja.Value {
				promise, resolve, _ := rt.NewPromise()
				go func() {
					stopOnce.Do(func() { close(stopped) })
					_ = srv.Shutdown(v.ctx)
					v.schedule(func() { _ = resolve(goja.Undefined()) })
				}()
				return rt.ToValue(promise)
			})
			return handle
		})
	})
}
