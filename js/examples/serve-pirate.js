// serve-pirate.js -- A pirate joke server: fetch, pipeline, serve.
//
//   ./js/bin/dbsp js/examples/serve-pirate.js
//   curl -s http://127.0.0.1:8000/
//   curl -s -X POST -H 'content-type: application/json' -b 'parrot=polly' -d '{"ship":"pearl"}' 'http://127.0.0.1:8000/ahoy?x=1'
//
// Each request fetches a dad joke from icanhazdadjoke.com (free, no API
// key), publishes it into a pipeline that translates it to pirate speak with
// @pirate, a custom expression registered from JS, and answers with what
// comes out of the pipeline. Stop it with Ctrl-C.

const WORDS = {
  hello: "ahoy", hi: "ahoy", my: "me", you: "ye", your: "yer", "you're": "ye be", "i'm": "I be",
  is: "be", are: "be", am: "be", the: "th'", friend: "matey", friends: "hearties", yes: "aye",
  money: "doubloons", stop: "avast", food: "grub", dad: "cap'n", man: "scallywag", boy: "cabin boy",
  wife: "wench", drink: "grog", water: "briny deep", wow: "shiver me timbers", and: "an'", of: "o'",
  for: "fer", to: "t'", with: "wit'", there: "thar", where: "whar", was: "be", were: "be", get: "git",
  car: "ship", house: "ship", happy: "jolly", bad: "scurvy", old: "barnacle-covered", over: "o'er",
};
expression.register("@pirate", (s) =>
  s.replace(/[A-Za-z']+/g, (w) => {
    const p = WORDS[w.toLowerCase()] || w.replace(/ing$/, "in'");
    return w[0] === w[0].toUpperCase() ? p[0].toUpperCase() + p.slice(1) : p;
  }) + " Arrr!");

aggregate
  .compile(
    [
      {
        "@project": {
          joke: "$.joke",
          pirate: { "@pirate": "$.joke" },
        },
      },
    ],
    { inputs: "jokes", outputs: ["pirate-jokes"] },
  )
  .commit();

const options = {
  port: 8000,
  hostname: "127.0.0.1",
  onListen: (addr) => console.log(`Ahoy on http://${addr.hostname}:${addr.port}/`),
};

// For simplicity this assumes one request at a time: concurrent requests
// interleave at the awaits and may receive each other's jokes.
serve(options, async (req, info) => {
  const url = new URL(req.url);

  // Log requests
  console.log(`${req.method} ${url.pathname} query=${url.search} from ${info.remoteAddr.hostname}:${info.remoteAddr.port}`);
  req.headers.forEach((value, name) => console.log(`  ${name}: ${value}`));
  console.log(`  cookies: ${req.headers.get("cookie") || "(none)"}`);
  if (req.method !== "GET") console.log(`  body: ${await req.text()}`);

  const res = await fetch("https://icanhazdadjoke.com/", { headers: { accept: "application/json" } });
  const { joke } = await res.json();

  const translated = subscribe.once("pirate-jokes");
  publish("jokes", [[{ joke }, 1]]);
  const [[row]] = await translated;
  return Response.json(row);
});
