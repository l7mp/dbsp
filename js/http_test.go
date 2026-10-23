package js

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/dop251/goja"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("fetch", func() {
	var (
		vm  *VM
		srv *httptest.Server
		// seen records the last request the server handled.
		seen chan *http.Request
		body chan string
		gone chan struct{}
	)

	BeforeEach(func() {
		seen = make(chan *http.Request, 8)
		body = make(chan string, 8)
		gone = make(chan struct{}, 8)
		mux := http.NewServeMux()
		mux.HandleFunc("/json", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"temp":71.6,"city":"Budapest"}`))
		})
		mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			seen <- r
			body <- string(b)
			w.Header().Set("X-Method", r.Method)
			w.Header().Add("X-Multi", "a")
			w.Header().Add("X-Multi", "b")
			_, _ = w.Write(b)
		})
		missing := func(w http.ResponseWriter, r *http.Request) {
			seen <- r
			http.NotFound(w, r)
		}
		mux.HandleFunc("/missing", missing)
		mux.HandleFunc("/missing/", missing)
		mux.HandleFunc("/binary", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte{0, 1, 2, 254, 255})
		})
		mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/json", http.StatusFound)
		})
		mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
			seen <- r
			select {
			case <-r.Context().Done():
				gone <- struct{}{}
			case <-time.After(5 * time.Second):
			}
		})
		srv = httptest.NewServer(mux)

		var err error
		vm, err = NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		vm.Close()
		srv.Close()
	})

	run := func(script string) error {
		return runScriptAsModule(vm, fmt.Sprintf("const BASE = %q;\nconst assert = require(\"assert\");\n%s", srv.URL, script))
	}

	It("GETs a JSON body", func() {
		Expect(run(`
const res = await fetch(BASE + "/json");
assert.strictEqual(res.status, 200);
assert.strictEqual(res.ok, true);
assert.strictEqual(res.statusText, "OK");
assert.strictEqual(res.headers.get("Content-Type"), "application/json");
const data = await res.json();
assert.strictEqual(data.temp, 71.6);
assert.strictEqual(data.city, "Budapest");
`)).To(Succeed())
	})

	It("accepts a URL as input and is reachable through require", func() {
		Expect(run(`
const f = require("http");
const res = await f.fetch(new URL("/json", BASE));
assert.strictEqual(res.ok, true);
assert.strictEqual(f.Headers, Headers);
`)).To(Succeed())
	})

	It("PUTs a string body with headers", func() {
		Expect(run(`
const res = await fetch(BASE + "/echo", {
  method: "put",
  headers: { "X-Token": "secret", "Content-Type": "application/json" },
  body: JSON.stringify({ a: 1 }),
});
assert.strictEqual(res.headers.get("x-method"), "PUT");
assert.strictEqual(res.headers.get("X-MULTI"), "a, b");
assert.strictEqual(res.headers.has("x-multi"), true);
assert.strictEqual(await res.text(), '{"a":1}');
`)).To(Succeed())
		var r *http.Request
		Eventually(seen).Should(Receive(&r))
		Expect(r.Method).To(Equal("PUT"))
		Expect(r.Header.Get("X-Token")).To(Equal("secret"))
		Expect(r.Header.Get("Content-Type")).To(Equal("application/json"))
		Expect(<-body).To(Equal(`{"a":1}`))
	})

	It("sends a form body for URLSearchParams", func() {
		Expect(run(`
await fetch(BASE + "/echo", { method: "POST", body: new URLSearchParams({ q: "a b", n: "1" }) });
`)).To(Succeed())
		var r *http.Request
		Eventually(seen).Should(Receive(&r))
		Expect(r.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded;charset=UTF-8"))
		Expect(<-body).To(Equal("q=a+b&n=1"))
	})

	It("rejects an unsupported body and a GET with a body", func() {
		Expect(run(`
await assert.rejects(fetch(BASE + "/echo", { method: "POST", body: { a: 1 } }));
await assert.rejects(fetch(BASE + "/echo", { body: "x" }));
`)).To(Succeed())
	})

	It("resolves a DELETE on a 404 with ok false", func() {
		Expect(run(`
const res = await fetch(BASE + "/missing", { method: "DELETE" });
assert.strictEqual(res.status, 404);
assert.strictEqual(res.ok, false);
`)).To(Succeed())
		var r *http.Request
		Eventually(seen).Should(Receive(&r))
		Expect(r.Method).To(Equal("DELETE"))
		Expect(r.ContentLength).To(BeZero())
	})

	It("round-trips binary bodies", func() {
		Expect(run(`
const res = await fetch(BASE + "/binary");
const bytes = new Uint8Array(await res.arrayBuffer());
assert.deepStrictEqual(Array.from(bytes), [0, 1, 2, 254, 255]);
const echo = await fetch(BASE + "/echo", { method: "POST", body: new Uint8Array([9, 8, 7]) });
assert.deepStrictEqual(Array.from(new Uint8Array(await echo.arrayBuffer())), [9, 8, 7]);
`)).To(Succeed())
	})

	It("rejects a second body read", func() {
		Expect(run(`
const res = await fetch(BASE + "/json");
assert.strictEqual(res.bodyUsed, false);
await res.text();
assert.strictEqual(res.bodyUsed, true);
let err;
try { await res.json(); } catch (e) { err = e; }
assert.ok(err instanceof TypeError, "second read must reject with TypeError");
`)).To(Succeed())
	})

	It("follows redirects and reports the final URL", func() {
		Expect(run(`
const res = await fetch(BASE + "/redirect");
assert.strictEqual(res.url, BASE + "/json");
assert.strictEqual((await res.json()).city, "Budapest");
`)).To(Succeed())
	})

	It("rejects a transport error with TypeError", func() {
		closed := httptest.NewServer(http.NotFoundHandler())
		url := closed.URL
		closed.Close()
		Expect(run(fmt.Sprintf(`
let err;
try { await fetch(%q); } catch (e) { err = e; }
assert.ok(err instanceof TypeError, "transport error must be a TypeError");
assert.strictEqual(err.message, "fetch failed");
assert.ok(err.cause, "the Go error is the cause");
`, url))).To(Succeed())
	})

	It("times out with AbortSignal.timeout and drops the connection", func() {
		Expect(run(`
let err;
try { await fetch(BASE + "/slow", { signal: AbortSignal.timeout(50) }); } catch (e) { err = e; }
assert.strictEqual(err.name, "TimeoutError");
`)).To(Succeed())
		Eventually(gone).Should(Receive())
	})

	It("aborts mid-request and refuses an already aborted signal", func() {
		Expect(run(`
const ac = new AbortController();
let fired = 0;
ac.signal.addEventListener("abort", () => fired++);
const p = fetch(BASE + "/slow", { signal: ac.signal });
setTimeout(() => ac.abort(), 50);
let err;
try { await p; } catch (e) { err = e; }
assert.strictEqual(err.name, "AbortError");
assert.strictEqual(fired, 1);
assert.strictEqual(ac.signal.aborted, true);

const reason = new Error("custom");
let err2;
try { await fetch(BASE + "/missing", { signal: AbortSignal.abort(reason) }); } catch (e) { err2 = e; }
assert.strictEqual(err2, reason);
assert.throws(() => AbortSignal.abort().throwIfAborted());
assert.throws(() => new AbortSignal());
`)).To(Succeed())
		Eventually(gone).Should(Receive())
		Expect(seen).To(HaveLen(1), "the pre-aborted request must not be sent")
	})

	It("shuts down cleanly with a request in flight", func() {
		Expect(runScript(vm, `fetch(`+fmt.Sprintf("%q", srv.URL+"/slow")+`).then(() => { throw new Error("late"); });`)).To(Succeed())
		Eventually(seen).Should(Receive())
		vm.Close()
		Eventually(gone).Should(Receive())
	})

	It("encodes path segments onto the wire and base64", func() {
		Expect(run(`
const { encodePath } = require("http");
const vhost = "/", name = "my policy";
assert.strictEqual(encodePath` + "`/api/policies/${vhost}/${name}`" + `, "/api/policies/%2F/my%20policy");
await fetch(BASE + encodePath` + "`/missing/${vhost}/${name}`" + `);
assert.strictEqual(btoa("guest:guest"), "Z3Vlc3Q6Z3Vlc3Q=");
assert.strictEqual(atob(btoa("guest:guest")), "guest:guest");
assert.strictEqual(decodeURIComponent(encodeURIComponent("a/b c")), "a/b c");
`)).To(Succeed())
		var r *http.Request
		Eventually(seen).Should(Receive(&r))
		Expect(r.RequestURI).To(Equal("/missing/%2F/my%20policy"))
	})

	It("constructs Headers from objects, pairs and other Headers", func() {
		Expect(run(`
const a = new Headers({ "Content-Type": "text/plain" });
a.append("x-a", "1");
a.append("X-A", "2");
const b = new Headers(a);
b.set("x-a", "3");
const c = new Headers([["X-B", "4"]]);
assert.strictEqual(a.get("x-a"), "1, 2");
assert.strictEqual(b.get("X-A"), "3");
assert.deepStrictEqual([...b.keys()], ["content-type", "x-a"]);
assert.deepStrictEqual([...c], [["x-b", "4"]]);
c.delete("x-b");
assert.strictEqual(c.get("x-b"), null);
const seen = [];
a.forEach((v, k) => seen.push(k + "=" + v));
assert.deepStrictEqual(seen, ["content-type=text/plain", "x-a=1, 2"]);
`)).To(Succeed())
	})
})

var _ = Describe("fetch over TLS", func() {
	var srv *httptest.Server

	BeforeEach(func() {
		srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"tls": true})
		}))
	})
	AfterEach(func() { srv.Close() })

	run := func(script string) error {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()
		return runScriptAsModule(vm, fmt.Sprintf("const BASE = %q;\nconst assert = require(\"assert\");\n%s", srv.URL, script))
	}

	It("rejects a self-signed certificate by default", func() {
		Expect(run(`await assert.rejects(fetch(BASE));`)).To(Succeed())
	})

	It("accepts it with tls.rejectUnauthorized false", func() {
		Expect(run(`
const res = await fetch(BASE, { tls: { rejectUnauthorized: false } });
assert.strictEqual((await res.json()).tls, true);
`)).To(Succeed())
	})

	Context("with NODE_TLS_REJECT_UNAUTHORIZED=0", func() {
		BeforeEach(func() {
			Expect(os.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", "0")).To(Succeed())
			DeferCleanup(os.Unsetenv, "NODE_TLS_REJECT_UNAUTHORIZED")
		})

		It("accepts it, and a per-request setting wins", func() {
			Expect(run(`
const res = await fetch(BASE);
assert.strictEqual(res.ok, true);
await assert.rejects(fetch(BASE, { tls: { rejectUnauthorized: true } }));
`)).To(Succeed())
		})
	})
})

// selfSignedPEM returns a PEM certificate and key for 127.0.0.1.
func selfSignedPEM() (string, string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "dbsp-test"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	Expect(err).NotTo(HaveOccurred())
	keyDER, err := x509.MarshalECPrivateKey(key)
	Expect(err).NotTo(HaveOccurred())
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

var _ = Describe("serve", func() {
	var vm *VM

	BeforeEach(func() {
		var err error
		vm, err = NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() { vm.Close() })

	run := func(script string) error {
		return runScriptAsModule(vm, "const assert = require(\"assert\");\n"+script)
	}

	// port reads the port a script stored in globalThis.port.
	port := func() int {
		var p int
		Expect(vm.runOnLoopSync(func(rt *goja.Runtime) error {
			p = int(rt.Get("port").ToInteger())
			return nil
		})).To(Succeed())
		return p
	}

	It("answers a fetch from the same script", func() {
		Expect(run(`
const server = serve({ port: 0, onListen() {} }, async (req, info) => {
  const body = await req.json();
  return Response.json({
    method: req.method,
    url: req.url,
    token: req.headers.get("X-Token"),
    host: req.headers.get("host"),
    from: info.remoteAddr.hostname,
    hello: body.name,
  }, { status: 201, headers: { "x-served-by": "dbsp" } });
});
assert.strictEqual(server.addr.hostname, "127.0.0.1");
const base = "http://127.0.0.1:" + server.addr.port;
const res = await fetch(base + "/greet?x=1", {
  method: "POST",
  headers: { "x-token": "secret" },
  body: JSON.stringify({ name: "gabor" }),
});
assert.strictEqual(res.status, 201);
assert.strictEqual(res.headers.get("x-served-by"), "dbsp");
assert.strictEqual(res.headers.get("content-type"), "application/json");
const got = await res.json();
assert.strictEqual(got.method, "POST");
assert.strictEqual(got.url, base + "/greet?x=1");
assert.strictEqual(got.token, "secret");
assert.strictEqual(got.host, "127.0.0.1:" + server.addr.port);
assert.strictEqual(got.from, "127.0.0.1");
assert.strictEqual(got.hello, "gabor");
await server.shutdown();
await server.finished;
`)).To(Succeed())
	})

	It("serves an outside client and keeps the raw path", func() {
		Expect(run(`
const server = serve({ port: 0, onListen(addr) { globalThis.port = addr.port; } },
  (req) => new Response("you asked for " + new URL(req.url).search + " via " + req.url.split("?")[0].split("/").slice(3).join("/")));
`)).To(Succeed())
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/a/%%2F/b?q=1", port()))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		Expect(resp.StatusCode).To(Equal(200))
		Expect(resp.Header.Get("Content-Type")).To(Equal("text/plain;charset=UTF-8"))
		Expect(string(b)).To(Equal("you asked for ?q=1 via a/%2F/b"))
	})

	It("answers 500 for a throwing handler or a non-Response, and runs onError", func() {
		Expect(run(`
const quiet = serve({ port: 0, onListen() {}, onError: (err) => new Response("oops: " + err.message, { status: 503 }) },
  () => { throw new Error("boom"); });
const res = await fetch("http://127.0.0.1:" + quiet.addr.port);
assert.strictEqual(res.status, 503);
assert.strictEqual(await res.text(), "oops: boom");

const bad = serve({ port: 0, onListen() {}, onError: () => { throw new Error("worse"); } }, () => 42);
assert.strictEqual((await fetch("http://127.0.0.1:" + bad.addr.port)).status, 500);
await quiet.shutdown();
await bad.shutdown();
`)).To(Succeed())
	})

	It("stops on shutdown and on an aborted signal", func() {
		Expect(run(`
const ac = new AbortController();
const server = serve({ port: 0, onListen() {}, signal: ac.signal, handler: () => new Response("hi") });
const url = "http://127.0.0.1:" + server.addr.port;
assert.strictEqual(await (await fetch(url)).text(), "hi");
ac.abort();
await server.finished;
await assert.rejects(fetch(url));
`)).To(Succeed())
	})

	It("closes with the VM", func() {
		Expect(run(`serve({ port: 0, onListen(addr) { globalThis.port = addr.port; } }, () => new Response("hi"));`)).To(Succeed())
		addr := fmt.Sprintf("127.0.0.1:%d", port())
		vm.Close()
		Eventually(func() error {
			c, err := net.Dial("tcp", addr)
			if err == nil {
				c.Close()
			}
			return err
		}).Should(HaveOccurred())
	})

	It("serves HTTPS with a PEM cert and key", func() {
		cert, key := selfSignedPEM()
		Expect(run(fmt.Sprintf(`
const server = serve({ port: 0, onListen() {}, cert: %q, key: %q }, (req) => new Response(req.url));
const url = "https://127.0.0.1:" + server.addr.port + "/secure";
await assert.rejects(fetch(url));
const res = await fetch(url, { tls: { rejectUnauthorized: false } });
assert.strictEqual(await res.text(), url);
await server.shutdown();
`, cert, key))).To(Succeed())
	})

	It("rejects bad options", func() {
		Expect(run(`
assert.throws(() => serve({ port: 0 }));
assert.throws(() => serve({ port: 0, cert: "x" }, () => new Response("")));
assert.throws(() => serve({ port: 0, cert: "x", key: "y", onListen() {} }, () => new Response("")));
`)).To(Succeed())
	})
})
