// fetch-weather.js -- Pull a public REST API into a DBSP pipeline.
//
//   ./js/bin/dbsp js/examples/fetch-weather.js
//
// Fetch the current temperature of Budapest in Fahrenheit from Open-Meteo
// (free, no API key), publish it into a pipeline that converts it to
// Celsius with @celsius, a custom expression registered from JS, and print
// the result.

expression.register("@celsius", (f) => Math.round(((f - 32) * 5) / 9 * 10) / 10);

aggregate
  .compile(
    [
      {
        "@project": {
          tempF: "$.tempF",
          tempC: { "@celsius": "$.tempF" },
        },
      },
    ],
    { inputs: "weather", outputs: ["celsius"] },
  )
  .commit();

subscribe("celsius", ([[row]]) => {
  console.log(`Budapest: ${row.tempF} F = ${row.tempC} C`);
  exit();
});

// The query goes in a form POST body; a GET with the same parameters in the
// URL works too.
fetch("https://api.open-meteo.com/v1/forecast", {
  method: "POST",
  headers: {
    "content-type": "application/x-www-form-urlencoded",
    accept: "application/json",
    "user-agent": "dbsp fetch example",
  },
  body: "latitude=47.5&longitude=19.04&current=temperature_2m&temperature_unit=fahrenheit",
})
  .then((res) => {
    console.log(`HTTP ${res.status} ${res.statusText}, content-type: ${res.headers.get("content-type")}`);
    return res.json();
  })
  .then((data) => publish("weather", [[{ tempF: data.current.temperature_2m }, 1]]));
