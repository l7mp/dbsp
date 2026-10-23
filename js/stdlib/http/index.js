"use strict";

// HTTP for DBSP scripts over the native @dbsp/http transport: a minimal
// subset of WHATWG fetch (fetch, Headers, Request, Response, AbortController,
// AbortSignal) to talk to REST APIs, and a Deno-style serve to answer them,
// with code that also runs unchanged on Node 18+ (fetch), Deno and Bun. Not
// supported: streaming bodies, FormData, cookies, and the
// redirect/credentials/cache modes (redirects are always followed).

const native = require("@dbsp/http");

// headerPairs lists the headers as [name, value] pairs, one per value, for
// the transport.
let headerPairs;

class Headers {
  #map = new Map(); // lower-cased name -> [values]

  constructor(init) {
    if (init == null) return;
    if (init instanceof Headers) {
      init.forEach((value, name) => this.append(name, value));
    } else if (Array.isArray(init)) {
      for (const pair of init) {
        if (!Array.isArray(pair) || pair.length !== 2) throw new TypeError("Headers: init pairs must be [name, value]");
        this.append(pair[0], pair[1]);
      }
    } else if (typeof init === "object") {
      for (const name of Object.keys(init)) this.append(name, init[name]);
    } else {
      throw new TypeError("Headers: init must be an object, an array of pairs or a Headers");
    }
  }

  append(name, value) {
    const key = String(name).toLowerCase();
    const values = this.#map.get(key);
    if (values) values.push(String(value));
    else this.#map.set(key, [String(value)]);
  }
  set(name, value) { this.#map.set(String(name).toLowerCase(), [String(value)]); }
  get(name) {
    const values = this.#map.get(String(name).toLowerCase());
    return values ? values.join(", ") : null;
  }
  has(name) { return this.#map.has(String(name).toLowerCase()); }
  delete(name) { this.#map.delete(String(name).toLowerCase()); }

  // Iteration is sorted by name with the values combined, as in the standard.
  *entries() {
    for (const name of [...this.#map.keys()].sort()) yield [name, this.#map.get(name).join(", ")];
  }
  *keys() { for (const [name] of this.entries()) yield name; }
  *values() { for (const [, value] of this.entries()) yield value; }
  [Symbol.iterator]() { return this.entries(); }
  forEach(fn, thisArg) {
    for (const [name, value] of this.entries()) fn.call(thisArg, value, name, this);
  }

  static {
    headerPairs = (headers) => {
      const out = [];
      for (const [name, values] of headers.#map) for (const value of values) out.push([name, value]);
      return out;
    };
  }
}

// toBytes encodes a body; strings are UTF-8.
function toBytes(body) {
  if (typeof body === "string") return new Uint8Array(Buffer.from(body, "utf8"));
  if (body instanceof ArrayBuffer) return new Uint8Array(body.slice(0));
  if (ArrayBuffer.isView(body)) return new Uint8Array(body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength));
  throw new TypeError("body must be a string, an ArrayBuffer, a typed array, a Buffer or URLSearchParams");
}

// bodyOf returns the bytes of a Request or Response without consuming it;
// setBody replaces them, for a received request whatever its method.
let bodyOf, setBody;

// Body holds the bytes shared by Request and Response and reads them once.
// A string body defaults the content-type to text/plain, URLSearchParams to
// a form; anything else needs JSON.stringify first, as in the standard.
class Body {
  #body;
  #bodyUsed = false;

  constructor(body, headers) {
    this.headers = new Headers(headers);
    if (body == null) {
      this.#body = new ArrayBuffer(0);
      return;
    }
    let type = null;
    if (body instanceof URLSearchParams) {
      body = body.toString();
      type = "application/x-www-form-urlencoded;charset=UTF-8";
    } else if (typeof body === "string") {
      type = "text/plain;charset=UTF-8";
    }
    if (type && !this.headers.has("content-type")) this.headers.set("content-type", type);
    this.#body = toBytes(body).buffer;
  }

  get bodyUsed() { return this.#bodyUsed; }

  #consume() {
    if (this.#bodyUsed) return Promise.reject(new TypeError("Body has already been consumed"));
    this.#bodyUsed = true;
    return Promise.resolve(this.#body);
  }
  arrayBuffer() { return this.#consume(); }
  text() { return this.#consume().then((ab) => Buffer.from(ab).toString("utf8")); }
  json() { return this.text().then((s) => JSON.parse(s)); }

  static {
    bodyOf = (b) => b.#body;
    setBody = (b, bytes) => { b.#body = bytes; };
  }
}

class Request extends Body {
  constructor(input, init = {}) {
    const from = input instanceof Request ? input : null;
    const method = init.method !== undefined ? String(init.method).toUpperCase() : from ? from.method : "GET";
    let body = init.body;
    if (body === undefined && from) body = bodyOf(from);
    if (body != null && (method === "GET" || method === "HEAD")) throw new TypeError(`Request with ${method} method cannot have body`);
    super(body, init.headers !== undefined ? init.headers : from ? from.headers : undefined);
    this.method = method;
    this.url = from ? from.url : String(input);
  }
}

class Response extends Body {
  constructor(body = null, init = {}) {
    super(body, init.headers);
    this.status = init.status === undefined ? 200 : init.status;
    this.statusText = init.statusText === undefined ? "" : String(init.statusText);
    this.url = init.url === undefined ? "" : String(init.url);
  }

  get ok() { return this.status >= 200 && this.status <= 299; }

  // Response.json(data, init) is a JSON response.
  static json(data, init = {}) {
    const headers = new Headers(init.headers);
    if (!headers.has("content-type")) headers.set("content-type", "application/json");
    return new Response(JSON.stringify(data), { ...init, headers });
  }
}

function abortError(name, message) {
  const err = new Error(message);
  err.name = name;
  return err;
}

const constructToken = Symbol("AbortSignal");
const listenersKey = Symbol("listeners");

// abortSignal marks signal aborted with reason (a default AbortError when
// undefined) and runs onabort and the listeners, once.
function abortSignal(signal, reason) {
  if (signal.aborted) return;
  signal.aborted = true;
  signal.reason = reason === undefined ? abortError("AbortError", "This operation was aborted") : reason;
  const event = { type: "abort", target: signal };
  const listeners = signal[listenersKey];
  signal[listenersKey] = [];
  for (const fn of [signal.onabort, ...listeners]) {
    if (typeof fn !== "function") continue;
    try {
      fn.call(signal, event);
    } catch (err) {
      console.error("AbortSignal listener threw:", err && err.stack ? err.stack : err);
    }
  }
}

class AbortSignal {
  constructor(token) {
    if (token !== constructToken) throw new TypeError("Illegal constructor");
    this.aborted = false;
    this.reason = undefined;
    this.onabort = null;
    Object.defineProperty(this, listenersKey, { value: [], writable: true });
  }

  addEventListener(type, fn) {
    if (type === "abort" && typeof fn === "function" && !this[listenersKey].includes(fn)) this[listenersKey].push(fn);
  }
  removeEventListener(type, fn) {
    if (type === "abort") this[listenersKey] = this[listenersKey].filter((l) => l !== fn);
  }
  throwIfAborted() {
    if (this.aborted) throw this.reason;
  }

  static abort(reason) {
    const signal = new AbortSignal(constructToken);
    abortSignal(signal, reason);
    return signal;
  }
  static timeout(ms) {
    const signal = new AbortSignal(constructToken);
    setTimeout(() => abortSignal(signal, abortError("TimeoutError", "The operation was aborted due to timeout")), ms);
    return signal;
  }
}

class AbortController {
  constructor() { this.signal = new AbortSignal(constructToken); }
  abort(reason) { abortSignal(this.signal, reason); }
}

// fetch(input, init) sends one HTTP request. input is a string, a URL or a
// Request; init takes method, headers, body, signal and tls
// ({rejectUnauthorized}, Bun's extension). An HTTP error status resolves; a
// transport error rejects with TypeError("fetch failed"), an abort with the
// signal's reason.
function fetch(input, init = {}) {
  return new Promise((resolve, reject) => {
    const signal = init.signal;
    if (signal && signal.aborted) {
      reject(signal.reason);
      return;
    }
    const req = new Request(input, init);

    let insecure = process.env.NODE_TLS_REJECT_UNAUTHORIZED === "0";
    if (init.tls && typeof init.tls.rejectUnauthorized === "boolean") insecure = !init.tls.rejectUnauthorized;

    const body = bodyOf(req);
    const h = native.request({
      method: req.method,
      url: req.url,
      headers: headerPairs(req.headers),
      body: body.byteLength > 0 ? body : undefined,
      insecure,
    });
    const onAbort = () => {
      h.cancel();
      reject(signal.reason);
    };
    if (signal) signal.addEventListener("abort", onAbort);
    h.promise.then(
      (res) => {
        if (signal) signal.removeEventListener("abort", onAbort);
        resolve(new Response(res.body, res));
      },
      (err) => {
        if (signal) signal.removeEventListener("abort", onAbort);
        reject(err);
      },
    );
  });
}

// serve(options, handler) answers HTTP requests, as Deno.serve: the handler
// gets a Request and an info object ({remoteAddr}) and returns a Response or
// a Promise of one. Options: port (8000; 0 picks a free one), hostname
// ("127.0.0.1"), cert and key (PEM strings, for HTTPS), onListen(addr)
// (default: log the address), onError(err) (returns the Response for a
// handler that threw; default: log and answer 500), signal (an AbortSignal
// that shuts the server down), and handler, in place of the second
// argument. Returns {addr, finished, shutdown()}: shutdown stops accepting
// and resolves once the in-flight requests are answered, as does finished.
function serve(options, handler) {
  if (typeof options === "function") [options, handler] = [{}, options];
  options = options || {};
  handler = handler || options.handler;
  if (typeof handler !== "function") throw new TypeError("serve: handler must be a function");
  if (!options.cert !== !options.key) throw new TypeError("serve: cert and key must be given together");

  const onError = options.onError || ((err) => {
    console.error("serve: handler failed:", err && err.stack ? err.stack : err);
    return new Response("Internal Server Error", { status: 500 });
  });
  const answer = (respond, res) => {
    if (!(res instanceof Response)) throw new TypeError("serve: handler must return a Response");
    respond({ status: res.status, headers: headerPairs(res.headers), body: bodyOf(res) });
  };

  const ln = native.listen(
    {
      hostname: options.hostname === undefined ? "127.0.0.1" : String(options.hostname),
      port: options.port === undefined ? 8000 : Number(options.port),
      cert: options.cert,
      key: options.key,
    },
    (raw, respond) => {
      const req = new Request(raw.url, { method: raw.method, headers: raw.headers });
      setBody(req, raw.body);
      Promise.resolve()
        .then(() => handler(req, { remoteAddr: raw.remoteAddr }))
        .then((res) => answer(respond, res))
        .catch((err) => Promise.resolve().then(() => onError(err)).then((res) => answer(respond, res)))
        .catch(() => respond({ status: 500, body: "Internal Server Error" }));
    },
  );

  const addr = { hostname: ln.hostname, port: ln.port, transport: "tcp" };
  const scheme = options.cert ? "https" : "http";
  (options.onListen || ((a) => console.log(`Listening on ${scheme}://${a.hostname}:${a.port}/`)))(addr);

  let resolveFinished;
  const finished = new Promise((resolve) => { resolveFinished = resolve; });
  const shutdown = () => ln.shutdown().then(resolveFinished);
  if (options.signal) {
    if (options.signal.aborted) shutdown();
    else options.signal.addEventListener("abort", shutdown);
  }
  return { addr, finished, shutdown };
}

// encodePath is a template tag that URI-encodes every interpolated value and
// leaves the literal parts alone: fetch(base + encodePath`/api/${vhost}/${name}`)
// puts the vhost "/" on the wire as %2F. Concatenate instead of resolving with
// new URL(path, base): the goja URL class normalizes the path, decoding %2F
// and dropping trailing slashes.
function encodePath(strings, ...values) {
  let out = strings[0];
  for (let i = 0; i < values.length; i++) out += encodeURIComponent(String(values[i])) + strings[i + 1];
  return out;
}

module.exports = { fetch, serve, Headers, Request, Response, AbortController, AbortSignal, encodePath };
